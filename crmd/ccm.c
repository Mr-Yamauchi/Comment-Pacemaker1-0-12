/* 
 * Copyright (C) 2004 Andrew Beekhof <andrew@beekhof.net>
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

/* put these first so that uuid_t is defined without conflicts */
#include <crm_internal.h>

#if SUPPORT_HEARTBEAT
#include <ocf/oc_event.h>
#include <ocf/oc_membership.h>
#endif

#include <string.h>

#include <crm/crm.h>
#include <crm/cib.h>
#include <crm/msg_xml.h>
#include <crm/common/xml.h>
#include <crm/common/cluster.h>
#include <crmd_messages.h>
#include <crmd_fsa.h>
#include <fsa_proto.h>
#include <crmd_callbacks.h>
#include <tengine.h>

gboolean membership_flux_hack = FALSE;
void post_cache_update(int instance);

#if SUPPORT_HEARTBEAT
oc_ev_t *fsa_ev_token;
void oc_ev_special(const oc_ev_t *, oc_ev_class_t , int );

void crmd_ccm_msg_callback(
    oc_ed_t event, void *cookie, size_t size, const void *data);
#endif

void ghash_update_cib_node(gpointer key, gpointer value, gpointer user_data);
void check_dead_member(const char *uname, GHashTable *members);
void reap_dead_ccm_nodes(gpointer key, gpointer value, gpointer user_data);

#define CCM_EVENT_DETAIL 0
#define CCM_EVENT_DETAIL_PARTIAL 0

int num_ccm_register_fails = 0;
int max_ccm_register_fails = 30;
int last_peer_update = 0;

extern GHashTable *voted;

struct update_data_s
{
		const char *state;
		const char *caller;
		xmlNode *updates;
		gboolean    overwrite_join;
};

/* �e�[�u���f�[�^����dead�����o�[����菜�� */
void reap_dead_ccm_nodes(gpointer key, gpointer value, gpointer user_data)
{
    crm_node_t *node = value;
    if(crm_is_member_active(node) == FALSE) {
		/* �f�[�^�̃m�[�h��񂪃A�N�e�B�u�ȃ����o�[�ł͂Ȃ��ꍇ */
		/* �f�b�h�����o�[�`�F�b�N���s�� */
		check_dead_member(node->uname, NULL);
		
		fail_incompletable_actions(transition_graph, node->uuid);
    }
}

extern gboolean check_join_state(enum crmd_fsa_state cur_state, const char *source);

/*
	�f�b�h�����o�[�`�F�b�N���� 
*/
void
check_dead_member(const char *uname, GHashTable *members)
{
	CRM_CHECK(uname != NULL, return);
	if(members != NULL && g_hash_table_lookup(members, uname) != NULL) {
		crm_err("%s didnt really leave the membership!", uname);
		return;
	}
	/* 	�e��n�b�V���e�[�u������Ώۃm�[�h�����폜���� */
	erase_node_from_join(uname);
	if(voted != NULL) {
		/* vote�n�b�V���e�[�u��������폜���� */
		g_hash_table_remove(voted, uname);
	}
	
	if(safe_str_eq(fsa_our_uname, uname)) {
		/* ���m�[�h�����X�g�����ꍇ */
		crm_err("We're not part of the cluster anymore");
		/* �������b�Z�[�W��I_ERROR���Z�b�g���� */
		register_fsa_input(C_FSA_INTERNAL, I_ERROR, NULL);

	} else if(AM_I_DC == FALSE && safe_str_eq(uname, fsa_our_dc)) {
		/* DC�m�[�h���A�N�e�B�u�ȃf�b�h�����o�[�̃m�[�h�������ꍇ */
		crm_warn("Our DC node (%s) left the cluster", uname);
		/* �������b�Z�[�W��I_ELECTION���Z�b�g���� */
		register_fsa_input(C_FSA_INTERNAL, I_ELECTION, NULL);

	} else if(fsa_state == S_INTEGRATION || fsa_state == S_FINALIZE_JOIN) {
		/* S_INTEGRATION��Ԃ��AS_FINALIZE_JOIN��Ԃ������ꍇ */
	    check_join_state(fsa_state, __FUNCTION__);
	}    
}

/*	 A_CCM_CONNECT	*/
/*	 CCM�֘A�̃A�N�V���������s����	*/
void
do_ccm_control(long long action,
		enum crmd_fsa_cause cause,
		enum crmd_fsa_state cur_state,
		enum crmd_fsa_input current_input,
		fsa_data_t *msg_data)
{	
#if SUPPORT_HEARTBEAT
    if(is_heartbeat_cluster()) {
	if(action & A_CCM_DISCONNECT){
		/* �A�N�V�������ؒf(A_CCM_DISCONNECT)�̏ꍇ�́AR_CCM_DISCONNECTED�r�b�g���Z�b�g���� */
		set_bit_inplace(fsa_input_register, R_CCM_DISCONNECTED);
		/* CCM�ؒf?? */
		oc_ev_unregister(fsa_ev_token);
	}

	if(action & A_CCM_CONNECT) {
		/* �A�N�V�������ڑ�(A_CCM_CONNECT)�̏ꍇ */
		int      ret;
		int	 fsa_ev_fd; 
		gboolean did_fail = FALSE;
		crm_debug_3("Registering with CCM");
		/* R_CCM_DISCONNECTED�r�b�g���N���A���� */
		clear_bit_inplace(fsa_input_register, R_CCM_DISCONNECTED);
		/* CCM�ڑ�?? */
		ret = oc_ev_register(&fsa_ev_token);
		if (ret != 0) {
			crm_warn("CCM registration failed");
			did_fail = TRUE;
		}

		if(did_fail == FALSE) {
			/* CCM�ɐڑ������ꍇ�AOC_EV_MEMB_CLASS�R�[���o�b�Ncrmd_ccm_msg_callback()���Z�b�g���� */
			crm_debug_3("Setting up CCM callbacks");
			ret = oc_ev_set_callback(fsa_ev_token, OC_EV_MEMB_CLASS,
						 crmd_ccm_msg_callback, NULL);
			if (ret != 0) {
				crm_warn("CCM callback not set");
				did_fail = TRUE;
			}
		}
		if(did_fail == FALSE) {
			oc_ev_special(fsa_ev_token, OC_EV_MEMB_CLASS, 0/*don't care*/);
			
			crm_debug_3("Activating CCM token");
			ret = oc_ev_activate(fsa_ev_token, &fsa_ev_fd);
			if (ret != 0){
				crm_warn("CCM Activation failed");
				did_fail = TRUE;
			}
		}

		if(did_fail) {
			/* CCM�ڑ��Ɏ��s�����ꍇ�́A�^�C�}�[(�g���K�[��)�𗘗p���ă��g���C���� */
			num_ccm_register_fails++;
			oc_ev_unregister(fsa_ev_token);
			
			if(num_ccm_register_fails < max_ccm_register_fails) {
				crm_warn("CCM Connection failed"
					 " %d times (%d max)",
					 num_ccm_register_fails,
					 max_ccm_register_fails);
				
				crm_timer_start(wait_timer);
				crmd_fsa_stall(NULL);
				return;
				
			} else {
				crm_err("CCM Activation failed %d (max) times",
					num_ccm_register_fails);
				register_fsa_error(C_FSA_INTERNAL, I_FAIL, NULL);
				return;
			}
		}
		

		crm_info("CCM connection established..."
			 " waiting for first callback");
		/* CCM����̐ڑ��Ď�??�R�[���o�b�N���Z�b�g���� */
		G_main_add_fd(G_PRIORITY_HIGH, fsa_ev_fd, FALSE, ccm_dispatch,
			      fsa_ev_token, default_ipc_connection_destroy);
		
	}
    }
#endif
    
    if(action & ~(A_CCM_CONNECT|A_CCM_DISCONNECT)) {
		/* �����o���Ȃ�ACTION�̏ꍇ�̓G���[ */
	crm_err("Unexpected action %s in %s",
		fsa_action2string(action), __FUNCTION__);
    }
}

#if SUPPORT_HEARTBEAT
/* CCM�C�x���g�̏ڍ׃��O�o�� */
void
ccm_event_detail(const oc_ev_membership_t *oc, oc_ed_t event)
{
	int lpc;
	gboolean member = FALSE;
	member = FALSE;

	crm_debug_2("-----------------------");
	crm_info("%s: trans=%d, nodes=%d, new=%d, lost=%d n_idx=%d, "
		 "new_idx=%d, old_idx=%d",
		 ccm_event_name(event),
		 oc->m_instance,
		 oc->m_n_member,
		 oc->m_n_in,
		 oc->m_n_out,
		 oc->m_memb_idx,
		 oc->m_in_idx,
		 oc->m_out_idx);
	
#if !CCM_EVENT_DETAIL_PARTIAL
	for(lpc=0; lpc < oc->m_n_member; lpc++) {
		crm_info("\tCURRENT: %s [nodeid=%d, born=%d]",
		       oc->m_array[oc->m_memb_idx+lpc].node_uname,
		       oc->m_array[oc->m_memb_idx+lpc].node_id,
		       oc->m_array[oc->m_memb_idx+lpc].node_born_on);

		if(safe_str_eq(fsa_our_uname,
			       oc->m_array[oc->m_memb_idx+lpc].node_uname)) {
			member = TRUE;
		}
	}
	if (member == FALSE) {
		crm_warn("MY NODE IS NOT IN CCM THE MEMBERSHIP LIST");
	}
#endif
	for(lpc=0; lpc<(int)oc->m_n_in; lpc++) {
		crm_info("\tNEW:     %s [nodeid=%d, born=%d]",
		       oc->m_array[oc->m_in_idx+lpc].node_uname,
		       oc->m_array[oc->m_in_idx+lpc].node_id,
		       oc->m_array[oc->m_in_idx+lpc].node_born_on);
	}
	
	for(lpc=0; lpc<(int)oc->m_n_out; lpc++) {
		crm_info("\tLOST:    %s [nodeid=%d, born=%d]",
		       oc->m_array[oc->m_out_idx+lpc].node_uname,
		       oc->m_array[oc->m_out_idx+lpc].node_id,
		       oc->m_array[oc->m_out_idx+lpc].node_born_on);
	}
	
	crm_debug_2("-----------------------");
	
}

#endif

gboolean ever_had_quorum = FALSE;
/*
	crm_peer_cache�L���b�V���ɕύX�����������Ƃ�ʒm����
*/
void
post_cache_update(int instance) 
{
    xmlNode *no_op = NULL;
    
    crm_peer_seq = instance;
    crm_debug("Updated cache after membership event %d.", instance);

	/* crm_peer_cache�n�b�V���e�[�u������dead�����o�[����菜�� */
    g_hash_table_foreach(crm_peer_cache, reap_dead_ccm_nodes, NULL);
    	
    set_bit_inplace(fsa_input_register, R_CCM_DATA);
    
    if(AM_I_DC) {
		/* ���m�[�h��DC�m�[�h�̏ꍇ��CIB��nodes����ݒ肷�� */
		populate_cib_nodes(FALSE);
		/* CIB��status�����X�V����*/
		do_update_cib_nodes(FALSE, __FUNCTION__);
    }

    /*
     * If we lost nodes, we should re-check the election status
     * Safe to call outside of an election
     */
    /* fsa_action��A_ELECTION_CHECK�A�N�V������ǉ����āAfsa_source�g���K�[��@����crmd�ɒʒm���� */
    register_fsa_action(A_ELECTION_CHECK);
    
    /* Membership changed, remind everyone we're here.
     * This will aid detection of duplicate DCs
     */
    /* CRM_OP_NOOP���b�Z�[�W�ŁAcrm_peer_cache�L���b�V���ɕύX�����������Ƃ��N���X�^�����o�[�Ƀ��b�Z�[�W�ʒm���� */
    no_op = create_request(
		CRM_OP_NOOP, NULL, NULL, CRM_SYSTEM_CRMD,
		AM_I_DC?CRM_SYSTEM_DC:CRM_SYSTEM_CRMD, NULL);
    send_cluster_message(NULL, crm_msg_crmd, no_op, FALSE);
    /* ���M���b�Z�[�W�̉�� */
    free_xml(no_op);
}


/*	 A_CCM_UPDATE_CACHE	*/
/*
 * Take the opportunity to update the node status in the CIB as well
 */
/*
   crm_peer_cache�L���b�V�����̃��X�^�ڑ��m�[�h���A�v���Z�X�����X�V����
*/
#if SUPPORT_HEARTBEAT
void
do_ccm_update_cache(
    enum crmd_fsa_cause cause, enum crmd_fsa_state cur_state,
    oc_ed_t event, const oc_ev_membership_t *oc, xmlNode *xml)
{
	unsigned long long instance = 0;
	unsigned int lpc = 0;
	
	if(is_heartbeat_cluster()) {
	    CRM_ASSERT(oc != NULL);
	    instance = oc->m_instance;
	}
	
	CRM_ASSERT(crm_peer_seq <= instance);

	switch(cur_state) {
	    case S_STOPPING:
	    case S_TERMINATE:
	    case S_HALT:
		crm_debug("Ignoring %s CCM event %llu, we're in state %s", 
			  ccm_event_name(event), instance,
			  fsa_state2string(cur_state));
		return;
	    case S_ELECTION:
    		/* fsa_action��A_ELECTION_CHECK�A�N�V������ǉ����āAfsa_source�g���K�[��@����crmd�ɒʒm���� */
			register_fsa_action(A_ELECTION_CHECK);
		break;
	    default:
		break;
	}
	
	if(is_heartbeat_cluster()) {
		/* CCM�C�x���g�̏ڍ׃��O�o�� */
	    ccm_event_detail(oc, event);
	    
	/*--*-- Recently Dead Member Nodes --*--*/
	    for(lpc=0; lpc < oc->m_n_out; lpc++) {
			/* lost�����m�[�h�̏�񂩂�N���X�^�ڑ��m�[�h���A�v���Z�X�����X�V���� */
			crm_update_ccm_node(oc, lpc+oc->m_out_idx, CRM_NODE_LOST, instance);
	    }
	    
	    /*--*-- All Member Nodes --*--*/
	    for(lpc=0; lpc < oc->m_n_member; lpc++) {
			/* �m�[�h�̏�񂩂�N���X�^�ڑ��m�[�h���A�v���Z�X�����X�V���� */
			crm_update_ccm_node(oc, lpc+oc->m_memb_idx, CRM_NODE_ACTIVE, instance);
	    }
	}

	if(event == OC_EV_MS_EVICTED) {
	    crm_update_peer(
		0, 0, 0, -1, 0,
		fsa_our_uuid, fsa_our_uname, NULL, CRM_NODE_EVICTED);

	    /* todo: drop back to S_PENDING instead */
	    /* get out... NOW!
	     *
	     * go via the error recovery process so that HA will
	     *    restart us if required
	     */
   		/* �����ŃG���[�������������������f�[�^��I_ERROR��o�^���� */
	    register_fsa_error_adv(cause, I_ERROR, NULL, NULL, __FUNCTION__);
	}

	/* crm_peer_cache�L���b�V���ɕύX�����������Ƃ�ʒm���� */
	post_cache_update(instance);
	return;
}
#endif
/*
	CIB����̍X�V�R�[���o�b�N
*/
static void
ccm_node_update_complete(xmlNode *msg, int call_id, int rc,
			 xmlNode *output, void *user_data)
{
	fsa_data_t *msg_data = NULL;
	last_peer_update = 0;

	/* �X�V���^�[���R�[�h�Ń��O���o�͂��� */
	if(rc == cib_ok) {
		/* ����I�� */
		crm_debug_2("Node update %d complete", call_id);

	} else {
		crm_err("Node update %d failed", call_id);
		crm_log_xml(LOG_DEBUG, "failed", msg);
		/* �����ŃG���[�������������������f�[�^��I_ERROR��o�^���� */
		register_fsa_error(C_FSA_INTERNAL, I_ERROR, NULL);
	}
}
/*
	�L���b�V���f�[�^����xml�f�[�^��ςݏグ����

	<node_state id="aef8bb90-2f33-4c8c-911a-3f37cf6f0157" uname="rh63-heartbeat1" ha="active" in_ccm="true" crmd="online" join="member" expected="member" c
rm-debug-origin="do_state_transition" shutdown="0">
		................
		................
	</node_state>
*/
void
ghash_update_cib_node(gpointer key, gpointer value, gpointer user_data)
{
    xmlNode *tmp1 = NULL;
    const char *join = NULL;
    crm_node_t *node = value;
    struct update_data_s* data = (struct update_data_s*)user_data;

    data->state = XML_BOOLEAN_NO;
    if(safe_str_eq(node->state, CRM_NODE_ACTIVE)) {
		data->state = XML_BOOLEAN_YES;
    }
    
    crm_debug("Updating %s: %s (overwrite=%s) hash_size=%d",
	      node->uname, data->state, data->overwrite_join?"true":"false",
	      g_hash_table_size(confirmed_nodes));
    
    if(data->overwrite_join) {
		if((node->processes & crm_proc_crmd) == FALSE) {
	    	join = CRMD_JOINSTATE_DOWN;
	    
		} else {
	    	const char *peer_member = g_hash_table_lookup(
			confirmed_nodes, node->uname);
	    	if(peer_member != NULL) {
				join = CRMD_JOINSTATE_MEMBER;
	    	} else {
				join = CRMD_JOINSTATE_PENDING;
	    	}
		}
    }
    /* �P�̃L���b�V���f�[�^���X�V�p�̍�Ɨpxml�ɍ쐬���� */
    tmp1 = create_node_state(
		node->uname, (node->processes&crm_proc_ais)?ACTIVESTATUS:DEADSTATUS,
		data->state, (node->processes&crm_proc_crmd)?ONLINESTATUS:OFFLINESTATUS,
		join, NULL, FALSE, data->caller);
    /* status�X�V�p��xml�ɍ�Ɨpxml��ǉ����� */
    add_node_copy(data->updates, tmp1);
    /* ��Ɨp��xml��������� */
    free_xml(tmp1);
}

/*
	CIB��status�����X�V����
	
	<status>
		<node_state id="aef8bb90-2f33-4c8c-911a-3f37cf6f0157" uname="rh63-heartbeat1" ha="active" in_ccm="true" crmd="online" join="member" expected="member" c
rm-debug-origin="do_state_transition" shutdown="0">
		................
		................
		</node_state>
		................
		................
	</status>		
*/
void
do_update_cib_nodes(gboolean overwrite, const char *caller)
{
    int call_id = 0;
    int call_options = cib_scope_local|cib_quorum_override;
    struct update_data_s update_data;
    xmlNode *fragment = NULL;
    
    if(crm_peer_cache == NULL) {
	/* We got a replace notification before being connected to
	 *   the CCM.
	 * So there is no need to update the local CIB with our values
	 *   - since we have none.
	 */
	 	/* crm_peer_cache���������̏ꍇ�͏������Ȃ� */
		return;
	
    } else if(AM_I_DC == FALSE) {
		/* ���m�[�h��DC�m�[�h�łȂ��ꍇ���������Ȃ� */
		return;
    }
    
    /* �X�V�p��status�m�[�h��XML�𐶐����� */
    fragment = create_xml_node(NULL, XML_CIB_TAG_STATUS);
    
    /* update�f�[�^���Z�b�g���� */
    update_data.caller = caller;
    update_data.updates = fragment;
    update_data.overwrite_join = overwrite;
    
    /* crm_peer_cashe�̑S�Ẵf�[�^���������� */
    /* --- �L���b�V���f�[�^����xml�f�[�^��ςݏグ���� --*/
    g_hash_table_foreach(crm_peer_cache, ghash_update_cib_node, &update_data);
    
    /* CIB��status�m�[�h���X�V���� */
    fsa_cib_update(XML_CIB_TAG_STATUS, fragment, call_options, call_id);
    /* CIB����̍X�V�ʒm�R�[���o�b�N���Z�b�g���� */
    add_cib_op_callback(fsa_cib_conn, call_id, FALSE, NULL, ccm_node_update_complete);
    last_peer_update = call_id;
    
    /* �X�V�p��XML��������� */
    free_xml(fragment);
}
/*
	CIB��hava-quorum�X�V�R�[���o�b�N 
*/
static void cib_quorum_update_complete(
    xmlNode *msg, int call_id, int rc, xmlNode *output, void *user_data)
{
	fsa_data_t *msg_data = NULL;
	
	/* �X�V���^�[���R�[�h�Ń��O���o�͂��� */
	if(rc == cib_ok) {
		/* ����I�� */
		crm_debug_2("Quorum update %d complete", call_id);

	} else {
		crm_err("Quorum update %d failed", call_id);
		crm_log_xml(LOG_DEBUG, "failed", msg);
		/* �����ŃG���[�������������������f�[�^��I_ERROR��o�^���� */
		register_fsa_error(C_FSA_INTERNAL, I_ERROR, NULL);
	}
} 
/*
	CIB��have_quorum�������X�V���� 
*/
void crm_update_quorum(gboolean quorum, gboolean force_update) 
{
    ever_had_quorum |= quorum;
    if(AM_I_DC && (force_update || fsa_has_quorum != quorum)) {
		/* ���m�[�h��DC��......�̏ꍇ */
		int call_id = 0;
		xmlNode *update = NULL;
		
		/* �I�v�V������ݒ肷�� */
		int call_options = cib_scope_local|cib_quorum_override;
	
		/* �X�V�p��xml�f�[�^���쎌���� */
		update = create_xml_node(NULL, XML_TAG_CIB);
		/* xml�f�[�^��hava-quorum�l���Z�b�g���� */
		crm_xml_add_int(update, XML_ATTR_HAVE_QUORUM, quorum);
		/* xml�f�[�^��dc-uuid�l���Z�b�g���� */
		set_uuid(update, XML_ATTR_DC_UUID, fsa_our_uname);
	
		/* cib���X�V����(modify API������ŌĂяo�� */
		fsa_cib_update(XML_TAG_CIB, update, call_options, call_id);
		crm_info("Updating quorum status to %s (call=%d)", quorum?"true":"false", call_id);
		/* �X�V�R�[���o�b�N���Z�b�g���� */
		add_cib_op_callback(fsa_cib_conn, call_id, FALSE, NULL, cib_quorum_update_complete);
		
		/* xml�f�[�^��������� */
		free_xml(update);
    }
    fsa_has_quorum = quorum;
}

