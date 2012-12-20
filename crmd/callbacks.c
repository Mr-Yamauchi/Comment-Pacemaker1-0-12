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

#include <crm_internal.h>

#include <sys/param.h>
#include <crm/crm.h>
#include <string.h>
#include <crmd_fsa.h>

#include <crm/msg_xml.h>
#include <crm/common/xml.h>
#include <crm/common/msg.h>
#include <crm/common/cluster.h>
#include <crm/cib.h>

#include <crmd.h>
#include <crmd_messages.h>
#include <crmd_callbacks.h>
#include <crmd_lrm.h>
#include <tengine.h>

void crmd_ha_connection_destroy(gpointer user_data);
void crmd_ha_msg_filter(xmlNode *msg);

/* From join_dc... */
extern gboolean check_join_state(
	enum crmd_fsa_state cur_state, const char *source);


#define trigger_fsa(source) crm_debug_3("Triggering FSA: %s", __FUNCTION__); \
	mainloop_set_trigger(source);
#if SUPPORT_HEARTBEAT
gboolean
crmd_ha_msg_dispatch(ll_cluster_t *cluster_conn, gpointer user_data)
{
	IPC_Channel *channel = NULL;
	gboolean stay_connected = TRUE;

	crm_debug_3("Invoked");

	if(cluster_conn != NULL) {
		channel = cluster_conn->llc_ops->ipcchan(cluster_conn);
	}
	
	CRM_CHECK(cluster_conn != NULL, ;);
	CRM_CHECK(channel != NULL, ;);
	
	if(channel != NULL && IPC_ISRCONN(channel)) {
		if(cluster_conn->llc_ops->msgready(cluster_conn) == 0) {
			crm_debug_2("no message ready yet");
		}
		/* invoke the callbacks but dont block */
		cluster_conn->llc_ops->rcvmsg(cluster_conn, 0);
	}
	
	if (channel == NULL || channel->ch_status != IPC_CONNECT) {
		if(is_set(fsa_input_register, R_HA_DISCONNECTED) == FALSE) {
			crm_crit("Lost connection to heartbeat service.");
		} else {
			crm_info("Lost connection to heartbeat service.");
		}
		trigger_fsa(fsa_source);
		stay_connected = FALSE;
	}
    
	return stay_connected;
}
#endif

void
crmd_ha_connection_destroy(gpointer user_data)
{
	crm_debug_3("Invoked");
	if(is_set(fsa_input_register, R_HA_DISCONNECTED)) {
		/* we signed out, so this is expected */
		crm_info("Heartbeat disconnection complete");
		return;
	}

	crm_crit("Lost connection to heartbeat service!");
	register_fsa_input(C_HA_DISCONNECT, I_ERROR, NULL);	
	trigger_fsa(fsa_source);
}

/* 
	Heartbeatからのクラスタメッセージを配送する
*/
void
crmd_ha_msg_filter(xmlNode *msg)
{
    if(AM_I_DC) {
		/* 自ノードがDCノードの場合 */
		const char *sys_from = crm_element_value(msg, F_CRM_SYS_FROM);
		if(safe_str_eq(sys_from, CRM_SYSTEM_DC)) {
			/* DCノードからの受信の場合 */
	    	const char *from = crm_element_value(msg, F_ORIG);
	    	if(safe_str_neq(from, fsa_our_uname)) {
				/* 自ノードがDCノードにも関わらず、他のDCノードから受信した場合 */
				int level = LOG_INFO;
				const char *op = crm_element_value(msg, F_CRM_TASK);

				/* make sure the election happens NOW */
				if(fsa_state != S_ELECTION) {
					/* S_ELECTION状態でなければエラー */
		    		ha_msg_input_t new_input;
		    		level = LOG_ERR;
		    		new_input.msg = msg;
		    		/* 処理でエラーが発生した内部処理データにI_ELECTIONを登録する */
		    		register_fsa_error_adv(
						C_FSA_INTERNAL, I_ELECTION, NULL, &new_input, __FUNCTION__);
				}
				/* 別のDCを検知したログを出力する */
				do_crm_log(level, "Another DC detected: %s (op=%s)", from, op);
				goto done;
	    	}
		}
    } else {
		/* 自ノードがDC以外の場合 */
		const char *sys_to = crm_element_value(msg, F_CRM_SYS_TO);
		if(safe_str_eq(sys_to, CRM_SYSTEM_DC)) {
			/* 宛先がDCの場合は処理しない */
	    	return;
		}
    }
    
    /* crm_log_xml(LOG_MSG, "HA[inbound]", msg); */
    route_message(C_HA_MESSAGE, msg);

  done:
    trigger_fsa(fsa_source);
}

#if SUPPORT_HEARTBEAT
/*
	Heartbeatからのクラスタメッセージ処理コールバック
*/
void
crmd_ha_msg_callback(HA_Message *hamsg, void* private_data)
{
	int level = LOG_DEBUG;
	crm_node_t *from_node = NULL;
	
	xmlNode *msg = convert_ha_message(NULL, hamsg, __FUNCTION__);
	const char *from = crm_element_value(msg, F_ORIG);
	const char *op   = crm_element_value(msg, F_CRM_TASK);
	const char *sys_from = crm_element_value(msg, F_CRM_SYS_FROM);

	CRM_CHECK(from != NULL, crm_log_xml_err(msg, "anon"); goto bail);

	crm_debug_2("HA[inbound]: %s from %s", op, from);

	if(crm_peer_cache == NULL || crm_active_members() == 0) {
		/* crm_peer_cacheキャッシュテーブルが未生成か、アクティブなノード数がまだ０の場合は処理しない */
		/* Heartbeat層でまだ構成が取れていないし、crmdも起動していない状態 */
		crm_debug("Ignoring HA messages until we are"
			  " connected to the CCM (%s op from %s)", op, from);
		crm_log_xml(LOG_MSG, "HA[inbound]: Ignore (No CCM)", msg);
		goto bail;
	}
	
	/* 送信元のノード情報を取り出す */
	from_node = crm_get_peer(0, from);
	if(crm_is_member_active(from_node) == FALSE) {
		/* アクティブメンバーからのメッセージでない場合は、ログを出力して、処理しない */
		if(safe_str_eq(op, CRM_OP_VOTE)) {
			/* CRM_OP_VOTEメッセージを受信した場合 */
			level = LOG_WARNING;

		} else if(AM_I_DC && safe_str_eq(op, CRM_OP_JOIN_ANNOUNCE)) {
			/* 自ノードがDCでCRM_OP_JOIN_ANNOUNCEを受信した場合 */
			level = LOG_WARNING;

		} else if(safe_str_eq(sys_from, CRM_SYSTEM_DC)) {
			level = LOG_WARNING;
		}
		do_crm_log(level, 
			   "Ignoring HA message (op=%s) from %s: not in our"
			   " membership list (size=%d)", op, from,
			   crm_active_members());
		
		crm_log_xml(LOG_MSG, "HA[inbound]: CCM Discard", msg);

	} else {
		/* アクティブなメンバーからのHeartbeat層からのクラスタメッセージを配送する */
	    crmd_ha_msg_filter(msg);
	}

  bail:
	free_xml(msg);
	return;
}
#endif


/*
 * Apparently returning TRUE means "stay connected, keep doing stuff".
 * Returning FALSE means "we're all done, close the connection"
 */
gboolean
crmd_ipc_msg_callback(IPC_Channel *client, gpointer user_data)
{
	int lpc = 0;
	xmlNode *msg = NULL;
	crmd_client_t *curr_client = (crmd_client_t*)user_data;
	gboolean stay_connected = TRUE;
	
	crm_debug_2("Invoked: %s",
		   curr_client->table_key);

	while(IPC_ISRCONN(client)) {
		if(client->ops->is_message_pending(client) == 0) {
			break;
		}

		msg = xmlfromIPC(client, MAX_IPC_DELAY);
		if (msg == NULL) {
		    break;
		}

		lpc++;
		crm_debug_2("Processing msg from %s", curr_client->table_key);
		crm_log_xml(LOG_DEBUG_2, "CRMd[inbound]", msg);

		if(crmd_authorize_message(msg, curr_client)) {
		    route_message(C_IPC_MESSAGE, msg);
		} 

		free_xml(msg);
		msg = NULL;

		if(client->ch_status != IPC_CONNECT) {
			break;
		}
	}
	
	crm_debug_2("Processed %d messages", lpc);
    
	if (client->ch_status != IPC_CONNECT) {
		stay_connected = FALSE;
		process_client_disconnect(curr_client);
	}

	trigger_fsa(fsa_source);
	return stay_connected;
}



extern GCHSource *lrm_source;

gboolean
lrm_dispatch(IPC_Channel *src_not_used, gpointer user_data)
{
	/* ?? src == lrm_channel ?? */
	ll_lrm_t *lrm = (ll_lrm_t*)user_data;
	IPC_Channel *lrm_channel = lrm->lrm_ops->ipcchan(lrm);

	lrm->lrm_ops->rcvmsg(lrm, FALSE);
	if(lrm_channel->ch_status != IPC_CONNECT) {
	    lrm_connection_destroy(NULL);
	    return FALSE;
	}
	return TRUE;
}

extern gboolean process_lrm_event(lrm_op_t *op);

/*
	LRMDからのOP通知コールバック処理
*/
void
lrm_op_callback(lrm_op_t* op)
{
	CRM_CHECK(op != NULL, return);
	/* LRMイベント処理を実行する */
	process_lrm_event(op);
}

void ais_status_callback(enum crm_status_type type, crm_node_t *node, const void *data) 
{
    gboolean reset_status_entry = FALSE;
    if(AM_I_DC == FALSE || node->uname == NULL) {
	return;
    }
    
    switch(type) {
	case crm_status_uname:
	    crm_info("status: %s is now %s", node->uname, node->state);
	    /* reset_status_entry = TRUE; */
	    /* If we've never seen the node, then it also wont be in the status section */
	    break;
	case crm_status_nstate:
	    crm_info("status: %s is now %s (was %s)", node->uname, node->state, (const char *)data);
	    reset_status_entry = TRUE;
	    break;
	case crm_status_processes:
	    break;
    }

    /* Can this be removed now that do_cl_join_finalize_respond() does the same thing? */
    if(reset_status_entry && safe_str_eq(CRMD_STATE_ACTIVE, node->state)) {
	erase_status_tag(node->uname, XML_CIB_TAG_LRM, cib_scope_local);
	erase_status_tag(node->uname, XML_TAG_TRANSIENT_NODEATTRS, cib_scope_local);
	/* TODO: potentially we also want to set XML_CIB_ATTR_JOINSTATE and XML_CIB_ATTR_EXPSTATE here */
    }
}

/*
	Heartbeatからのノード statusメッセージ受信コールバック 
	--- 他ノードでHeartbeatがオンライン・オフラインすると通知されるコールバック ----
*/
void
crmd_ha_status_callback(const char *node, const char *status, void *private)
{
	xmlNode *update = NULL;
	crm_node_t *member = NULL;
	crm_notice("Status update: Node %s now has status [%s] (DC=%s)",
		   node, status, AM_I_DC?"true":"false");
	
	/* statusメッセージを受信した内容について、crm_peer_cacheキャッシュから接続ノード情報を取得する */
	member = crm_get_peer(0, node);
	if(member == NULL || crm_is_member_active(member) == FALSE) {
		/* ノード情報がcrm_peer_cacheキャッシュに存在しないか */
		/* アクティブなメンバーではない場合 */
	    /* Make sure it is created so crm_update_peer_proc() succeeds */
	    const char *uuid = get_uuid(node);
      	/* クラスタに接続しているノード情報を更新する */
	    member = crm_update_peer(0, 0, 0, -1, 0, uuid, node, NULL, NULL);
	}

	if(safe_str_eq(status, PINGSTATUS)) {
		/* statusがPINGSTATUSの場合は処理しない */
	    return;
	}
	
	if(safe_str_eq(status, DEADSTATUS)) {
		/* statusメッセージがDEADSTATUSだった場合 */
	    /* this node is toast */
      	/* クラスタに接続しているプロセス情報をOFFLINESTATUSに更新する */
	    crm_update_peer_proc(node, crm_proc_ais, OFFLINESTATUS);
	    if(AM_I_DC) {
			/* 自ノードがDCノードの場合は、status更新用のXMLを生成する */
			update = create_node_state(
				node, DEADSTATUS, XML_BOOLEAN_NO, OFFLINESTATUS,
				CRMD_JOINSTATE_DOWN, NULL, TRUE, __FUNCTION__);
	    }
	    
	} else {
		/* statusメッセージがDEADSTATUS以外だった場合 */
      	/* クラスタに接続しているプロセス情報をONLINESTATUSに更新する */
	    crm_update_peer_proc(node, crm_proc_ais, ONLINESTATUS);
	    if(AM_I_DC) {
			/* 自ノードがDCノードの場合は、status更新用のXMLを生成する */
			update = create_node_state(
				node, ACTIVESTATUS, NULL, NULL,
				CRMD_JOINSTATE_PENDING, NULL, FALSE, __FUNCTION__);
	    }
	}

	/*
	   fsa_sourceトリガーを叩いて、CRMDにHeartbeatからのノード status受信があったことを通知する
	*/
	trigger_fsa(fsa_source);

	if(update != NULL) {
		/* 更新用のXMLが生成されている場合は、CIBを更新する */
	    fsa_cib_anon_update(
		XML_CIB_TAG_STATUS, update, cib_scope_local|cib_quorum_override|cib_can_create);
		/* 更新用のXMLを解放する */
	    free_xml(update);
	}
}

/*
	Heartbeatからの接続crmd CLIENT statusメッセージ受信コールバック
	--- 他ノードでcrmdが接続・切断すると通知されるコールバック ----
*/
void
crmd_client_status_callback(const char * node, const char * client,
			    const char * status, void * private)
{
	const char *join = NULL;
	crm_node_t *member = NULL;
	xmlNode *update = NULL;
	gboolean clear_shutdown = FALSE;
	
	crm_debug_3("Invoked");
	if(safe_str_neq(client, CRM_SYSTEM_CRMD)) {
		return;
	}

	if(safe_str_eq(status, JOINSTATUS)){
 		clear_shutdown = TRUE;
		status = ONLINESTATUS;
		join = CRMD_JOINSTATE_PENDING;

	} else if(safe_str_eq(status, LEAVESTATUS)){
		status = OFFLINESTATUS;
		join   = CRMD_JOINSTATE_DOWN;
/* 		clear_shutdown = TRUE; */
	}
	
	/* Heartbeatからの受信フラグ(R_PEER_DATA)をセットする */
	set_bit_inplace(fsa_input_register, R_PEER_DATA);

	crm_notice("Status update: Client %s/%s now has status [%s] (DC=%s)",
		   node, client, status, AM_I_DC?"true":"false");

	if(safe_str_eq(status, ONLINESTATUS)) {
	    /* remove the cached value in case it changed */
	    crm_debug_2("Uncaching UUID for %s", node);
	    unget_uuid(node);
	}

	member = crm_get_peer(0, node);
	if(member == NULL || crm_is_member_active(member) == FALSE) {
	    /* Make sure it is created so crm_update_peer_proc() succeeds */
	    const char *uuid = get_uuid(node);
	    member = crm_update_peer(0, 0, 0, -1, 0, uuid, node, NULL, NULL);
	}

	crm_update_peer_proc(node, crm_proc_crmd, status);
	
	if(is_set(fsa_input_register, R_CIB_CONNECTED) == FALSE) {
		return;
	} else if(fsa_state == S_STOPPING) {
		return;
	}
	
	if(safe_str_eq(node, fsa_our_dc) && safe_str_eq(status, OFFLINESTATUS)){
		/* did our DC leave us */
		/* クラスタで認識しているDCノードが、OFFLINESTATUSになった場合 */
		crm_info("Got client status callback - our DC is dead");
		/* I_ELECTIONへ */
		register_fsa_input(C_CRMD_STATUS_CALLBACK, I_ELECTION, NULL);
		
	} else if(AM_I_DC == FALSE) {
		crm_info("Not the DC");

	} else {
	    crm_debug_3("Got client status callback");
	    update = create_node_state(
		node, NULL, NULL, status, join, NULL, clear_shutdown, __FUNCTION__);
	    
	    fsa_cib_anon_update(
		XML_CIB_TAG_STATUS, update, cib_scope_local|cib_quorum_override|cib_can_create);
	    free_xml(update);
	    
	    if(safe_str_eq(status, OFFLINESTATUS)) {
			/* 	各種ハッシュテーブルから対象ノード情報を削除する */
			erase_node_from_join(node);
			check_join_state(fsa_state, __FUNCTION__);
			fail_incompletable_actions(transition_graph, member->uuid);
	    }
	}
	/*
	   fsa_sourceトリガーを叩いて、CRMDにHeartbeatからのClient status受信があったことを通知する
	*/
	trigger_fsa(fsa_source);
}

void
crmd_ipc_connection_destroy(gpointer user_data)
{
	GCHSource *source = NULL;
	crmd_client_t *client = user_data;

/* Calling this function on an _active_ connection results in:
 * crmd_ipc_connection_destroy (callbacks.c:431)
 * -> G_main_del_IPC_Channel (GSource.c:478)
 *  -> g_source_unref
 *   -> G_CH_destroy_int (GSource.c:647)
 *    -> crmd_ipc_connection_destroy (callbacks.c:437)\
 *
 * A better alternative is to call G_main_del_IPC_Channel() directly
 */

	if(client == NULL) {
		crm_debug_4("No client to delete");
		return;
	}

	crm_debug_2("Disconnecting client %s (%p)", client->table_key, client);
	source = client->client_source;
	client->client_source = NULL;
	if(source != NULL) {
		crm_debug_3("Deleting %s (%p) from mainloop",
			    client->table_key, source);
		G_main_del_IPC_Channel(source);
	} 
	crm_free(client->table_key);
	crm_free(client->sub_sys);
	crm_free(client->uuid);
	crm_free(client);
	
	return;
}

gboolean
crmd_client_connect(IPC_Channel *client_channel, gpointer user_data)
{
	crm_debug_3("Invoked");
	if (client_channel == NULL) {
		crm_err("Channel was NULL");

	} else if (client_channel->ch_status == IPC_DISCONNECT) {
		crm_err("Channel was disconnected");

	} else {
		crmd_client_t *blank_client = NULL;
		crm_debug_3("Channel connected");
		crm_malloc0(blank_client, sizeof(crmd_client_t));
		CRM_ASSERT(blank_client != NULL);

		crm_debug_2("Created client: %p", blank_client);
		
		client_channel->ops->set_recv_qlen(client_channel, 1024);
		client_channel->ops->set_send_qlen(client_channel, 1024);
	
		blank_client->client_channel = client_channel;
		blank_client->sub_sys   = NULL;
		blank_client->uuid      = NULL;
		blank_client->table_key = NULL;
	
		blank_client->client_source =
			G_main_add_IPC_Channel(
				G_PRIORITY_LOW, client_channel,
				FALSE,  crmd_ipc_msg_callback,
				blank_client, crmd_ipc_connection_destroy);
	}
    
	return TRUE;
}


#if SUPPORT_HEARTBEAT
static gboolean fsa_have_quorum = FALSE;
/*
	CCMとの接続監視用？？？のコールバック
*/
gboolean ccm_dispatch(int fd, gpointer user_data)
{
	int rc = 0;
	oc_ev_t *ccm_token = (oc_ev_t*)user_data;
	gboolean was_error = FALSE;
	
	crm_debug_3("Invoked");
	rc = oc_ev_handle_event(ccm_token);

	if(rc != 0) {
		if(is_set(fsa_input_register, R_CCM_DISCONNECTED) == FALSE) {
			/* we signed out, so this is expected */
			register_fsa_input(C_CCM_CALLBACK, I_ERROR, NULL);
			crm_err("CCM connection appears to have failed: rc=%d.",
				rc);
		}
		was_error = TRUE;
	}
	/*
	   fsa_sourceトリガーを叩いて、CRMDにCCMからの受信があったことを通知する
	*/
	trigger_fsa(fsa_source);
	return !was_error;
}
/*
	ccmからのOC_EV_MEMB_CLASS通知コールバック処理
*/

void 
crmd_ccm_msg_callback(
	oc_ed_t event, void *cookie, size_t size, const void *data)
{
	gboolean update_cache = FALSE;
	const oc_ev_membership_t *membership = data;

	gboolean update_quorum = FALSE;

	crm_debug_3("Invoked");
	CRM_ASSERT(data != NULL);
	
	crm_info("Quorum %s after event=%s (id=%d)", 
		 ccm_have_quorum(event)?"(re)attained":"lost",
		 ccm_event_name(event), membership->m_instance);

	if(crm_peer_seq > membership->m_instance) {
		crm_err("Membership instance ID went backwards! %llu->%d",
			crm_peer_seq, membership->m_instance);
		CRM_ASSERT(crm_peer_seq <= membership->m_instance);
		return;
	}
	
	/*
	 * OC_EV_MS_NEW_MEMBERSHIP:   membership with quorum(定数をもつ会員)
	 * OC_EV_MS_MS_INVALID:       membership without quorum(定数をもたない会員)
	 * OC_EV_MS_NOT_PRIMARY:      previous membership no longer valid(もはや有効でない前のメンバーシップ)
	 * OC_EV_MS_PRIMARY_RESTORED: previous membership restored(回復する前のメンバーシップ)
	 * OC_EV_MS_EVICTED:          the client is evicted from ccm.(クライアントは、ccmから追い出されます)
	 */
	
	switch(event) {
		case OC_EV_MS_NEW_MEMBERSHIP:
		case OC_EV_MS_INVALID:
			update_cache = TRUE;
			update_quorum = TRUE;
			break;
		case OC_EV_MS_NOT_PRIMARY:
			break;
		case OC_EV_MS_PRIMARY_RESTORED:
			update_cache = TRUE;
			crm_peer_seq = membership->m_instance;
			break;
		case OC_EV_MS_EVICTED:
			update_quorum = TRUE;
			/* 内部メッセージにI_STOPをセットする */
			register_fsa_input(C_FSA_INTERNAL, I_STOP, NULL);
			crm_err("Shutting down after CCM event: %s",
				ccm_event_name(event));
			break;
		default:
			crm_err("Unknown CCM event: %d", event);
	}

	if(update_quorum) {
		/* quorum更新が必要な場合 */
	    crm_have_quorum = ccm_have_quorum(event);
	    /* 自ノードがDC決定している場合は、CIBのhave_quorum属性を更新する */
	    /* していない場合は、fsa_has_quorumのみquorum保持フラグを更新 */
	    crm_update_quorum(crm_have_quorum, FALSE);

	    if(crm_have_quorum == FALSE) {
			/* quorumを消失してしまって、前のquorumが保持状態ならログを出力 */
			/* did we just loose quorum? */
			if(fsa_have_quorum) {
				/* この判定処理は間違いがあってパッチを1.0.13候補へ投稿済み。適用待ち */
		    	crm_info("Quorum lost: %s", ccm_event_name(event));
			}
	    }
	}
	
	if(update_cache) {
		/* cache更新が必要な場合 */
	    crm_debug_2("Updating cache after event %s", ccm_event_name(event));
	    /*    crm_peer_cacheキャッシュ内のラスタ接続ノード情報、プロセス情報を更新する */
	    do_ccm_update_cache(C_CCM_CALLBACK, fsa_state, event, data, NULL);

	} else if(event != OC_EV_MS_NOT_PRIMARY) {
	    crm_peer_seq = membership->m_instance;
        /* fsa_actionにA_TE_CANCELアクションを追加して、fsa_sourceトリガーを叩いてcrmdに通知する */
	    register_fsa_action(A_TE_CANCEL);
	}
	/* ccmコールバックの終了処理 */
	oc_ev_callback_done(cookie);
	return;
}
#endif

void
crmd_cib_connection_destroy(gpointer user_data)
{
    CRM_CHECK(user_data == fsa_cib_conn, ;);
    
	crm_debug_3("Invoked");
	trigger_fsa(fsa_source);
	fsa_cib_conn->state = cib_disconnected;
	
	if(is_set(fsa_input_register, R_CIB_CONNECTED) == FALSE) {
		crm_info("Connection to the CIB terminated...");
		return;
	}

	/* eventually this will trigger a reconnect, not a shutdown */ 
	crm_err("Connection to the CIB terminated...");
	register_fsa_input(C_FSA_INTERNAL, I_ERROR, NULL);
	clear_bit_inplace(fsa_input_register, R_CIB_CONNECTED);
	
	return;
}

/*
  トリガー処理
  
*/
gboolean
crm_fsa_trigger(gpointer user_data) 
{
	crm_debug_2("Invoked (queue len: %d)", g_list_length(fsa_message_queue));
	
	/* トリガー契機からの状態処理を行う */
	s_crmd_fsa(C_FSA_INTERNAL);
	crm_debug_2("Exited  (queue len: %d)", g_list_length(fsa_message_queue));
	return TRUE;	
}
