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

#include <crm/crm.h>
#include <crm/cib.h>
#include <crm/msg_xml.h>
#include <crm/common/xml.h>
#include <crm/common/cluster.h>

#include <crmd_fsa.h>
#include <crmd_messages.h>


GHashTable *welcomed_nodes   = NULL;
GHashTable *integrated_nodes = NULL;
GHashTable *finalized_nodes  = NULL;
GHashTable *confirmed_nodes  = NULL;
char *max_epoch = NULL;
char *max_generation_from = NULL;
xmlNode *max_generation_xml = NULL;

void initialize_join(gboolean before);
gboolean finalize_join_for(gpointer key, gpointer value, gpointer user_data);
void finalize_sync_callback(xmlNode *msg, int call_id, int rc,
			    xmlNode *output, void *user_data);
gboolean check_join_state(enum crmd_fsa_state cur_state, const char *source);

static int current_join_id = 0;
unsigned long long saved_ccm_membership_id = 0;

void
initialize_join(gboolean before)
{
	/* clear out/reset a bunch of stuff */
	crm_debug("join-%d: Initializing join data (flag=%s)",
		  current_join_id, before?"true":"false");
	
	g_hash_table_destroy(welcomed_nodes);
	g_hash_table_destroy(integrated_nodes);
	g_hash_table_destroy(finalized_nodes);
	g_hash_table_destroy(confirmed_nodes);

	if(before) {
		if(max_generation_from != NULL) {
			crm_free(max_generation_from);
			max_generation_from = NULL;
		}
		if(max_generation_xml != NULL) {
			free_xml(max_generation_xml);
			max_generation_xml = NULL;
		}
		clear_bit_inplace(fsa_input_register, R_HAVE_CIB);
		clear_bit_inplace(fsa_input_register, R_CIB_ASKED);
	}
	
	/* 各種ハッシュテーブルを作成する */
	welcomed_nodes = g_hash_table_new_full(
		g_str_hash, g_str_equal,
		g_hash_destroy_str, g_hash_destroy_str);
	integrated_nodes = g_hash_table_new_full(
		g_str_hash, g_str_equal,
		g_hash_destroy_str, g_hash_destroy_str);
	finalized_nodes = g_hash_table_new_full(
		g_str_hash, g_str_equal,
		g_hash_destroy_str, g_hash_destroy_str);
	confirmed_nodes = g_hash_table_new_full(
		g_str_hash, g_str_equal,
		g_hash_destroy_str, g_hash_destroy_str);
}

/*
	各種ハッシュテーブルから対象ノード情報を削除する
*/
void
erase_node_from_join(const char *uname) 
{
	gboolean w = FALSE, i = FALSE, f = FALSE, c = FALSE;
    
	if(uname == NULL) {
		return;
	}

	if(welcomed_nodes != NULL) {
	    w = g_hash_table_remove(welcomed_nodes, uname);
	}
	if(integrated_nodes != NULL) {
	    i = g_hash_table_remove(integrated_nodes, uname);
	}
	if(finalized_nodes != NULL) {
	    f = g_hash_table_remove(finalized_nodes, uname);
	}
	if(confirmed_nodes != NULL) {
	    c = g_hash_table_remove(confirmed_nodes, uname);
	}

	if(w || i || f || c) {
	    crm_info("Removed node %s from join calculations:"
		     " welcomed=%d itegrated=%d finalized=%d confirmed=%d",
		     uname, w, i, f, c);
	}
}

static void
join_make_offer(gpointer key, gpointer value, gpointer user_data)
{
	const char *join_to = NULL;
	const crm_node_t *member = value;

	CRM_ASSERT(member != NULL);
	if(crm_is_member_active(member) == FALSE) {
	    return;
	}

	join_to = member->uname;
	if(join_to == NULL) {
		crm_err("No recipient for welcome message");
		return;
	}

	/* 	各種ハッシュテーブルから対象ノード情報を削除する */
	erase_node_from_join(join_to);

	if(saved_ccm_membership_id != crm_peer_seq) {
		saved_ccm_membership_id = crm_peer_seq;
		crm_info("Making join offers based on membership %llu",
			 crm_peer_seq);
	}	
	
	if(member->processes & crm_proc_crmd) {
		/* CRM_OP_JOIN_OFFERメッセージを生成する */
		xmlNode *offer = create_request(
			CRM_OP_JOIN_OFFER, NULL, join_to,
			CRM_SYSTEM_CRMD, CRM_SYSTEM_DC, NULL);
		char *join_offered = crm_itoa(current_join_id);
		
		crm_xml_add_int(offer, F_CRM_JOIN_ID, current_join_id);
		/* send the welcome */
		crm_debug("join-%d: Sending offer to %s",
			  current_join_id, join_to);

		/* 対象ノードにCRM_OP_JOIN_OFFERメッセージを送信する */
		send_cluster_message(join_to, crm_msg_crmd, offer, TRUE);
		free_xml(offer);

		/* welcomed_nodesハッシュテーブルに対象ノードを保存 */
		g_hash_table_insert(
			welcomed_nodes, crm_strdup(join_to), join_offered);
	} else {
		crm_info("Peer process on %s is not active (yet?): %.8lx %d",
			 join_to, (long)member->processes,
			 g_hash_table_size(crm_peer_cache));
	}
	
}

/*	 A_DC_JOIN_OFFER_ALL	*/
/* A_DC_JOIN_OFFER_ALLアクション処理 */
/* 現在のDC情報をクリアして 																						*/
/* CRM_OP_JOIN_OFFERメッセージをcrm_peer_cacheキャッシュに認識されているクラスターメンバーのcrmdプロセスに送信する */
void
do_dc_join_offer_all(long long action,
		     enum crmd_fsa_cause cause,
		     enum crmd_fsa_state cur_state,
		     enum crmd_fsa_input current_input,
		     fsa_data_t *msg_data)
{
	/* reset everyones status back to down or in_ccm in the CIB
	 *
	 * any nodes that are active in the CIB but not in the CCM list
	 *   will be seen as offline by the PE anyway
	 */
	current_join_id++;
	initialize_join(TRUE);
/* 	do_update_cib_nodes(TRUE, __FUNCTION__); */

	/* 現在のDCをクリア */
	update_dc(NULL);
	if(cause == C_HA_MESSAGE && current_input == I_NODE_JOIN) {
	    crm_info("A new node joined the cluster");
	}
	
	/* rm_peer_cacheキャッシュに認識されているクラスターメンバーのcrmdプロセス */
	/* にCRM_OP_JOIN_OFFERメッセージを送信する */
	g_hash_table_foreach(crm_peer_cache, join_make_offer, NULL);
	
	/* dont waste time by invoking the PE yet; */
	/* CRM_OP_JOIN_OFFERメッセージを送信後、welcomed_nodesハッシュテーブルのメンバー数をログで出力 */
	crm_info("join-%d: Waiting on %d outstanding join acks",
		 current_join_id, g_hash_table_size(welcomed_nodes));
}

/*	 A_DC_JOIN_OFFER_ONE	*/
void
do_dc_join_offer_one(long long action,
		     enum crmd_fsa_cause cause,
		     enum crmd_fsa_state cur_state,
		     enum crmd_fsa_input current_input,
		     fsa_data_t *msg_data)
{
	crm_node_t *member;
	ha_msg_input_t *welcome = NULL;

	const char *op = NULL;
	const char *join_to = NULL;

	if(msg_data->data) {
	    welcome = fsa_typed_data(fsa_dt_ha_msg);

	} else {
	    crm_info("A new node joined - wait until it contacts us");
	    return;
	}
	
	if(welcome == NULL) {
		crm_err("Attempt to send welcome message "
			"without a message to reply to!");
		return;
	}
	
	join_to = crm_element_value(welcome->msg, F_CRM_HOST_FROM);
	if(join_to == NULL) {
	    crm_err("Attempt to send welcome message "
		    "without a host to reply to!");
	    return;
	}

	member = crm_get_peer(0, join_to);
	if(member == NULL || crm_is_member_active(member) == FALSE) {
	    crm_err("Attempt to send welcome message "
		    "to a node not part of our partition!");
	    return;
	}
	
	op = crm_element_value(welcome->msg, F_CRM_TASK);
	if(join_to != NULL
	   && (cur_state == S_INTEGRATION || cur_state == S_FINALIZE_JOIN)) {
		/* note: it _is_ possible that a node will have been
		 *  sick or starting up when the original offer was made.
		 *  however, it will either re-announce itself in due course
		 *  _or_ we can re-store the original offer on the client.
		 */
		crm_debug("(Re-)offering membership to %s...", join_to);
	}

	crm_info("join-%d: Processing %s request from %s in state %s",
		 current_join_id, op, join_to, fsa_state2string(cur_state));

	join_make_offer(NULL, member, NULL);
	
	/* always offer to the DC (ourselves)
	 * this ensures the correct value for max_generation_from
	 */
	member = crm_get_peer(0, fsa_our_uname);
	join_make_offer(NULL, member, NULL);	
	
	/* this was a genuine join request, cancel any existing
	 * transition and invoke the PE
	 */
	start_transition(fsa_state);
	
	/* dont waste time by invoking the pe yet; */
	crm_debug("Waiting on %d outstanding join acks for join-%d",
		  g_hash_table_size(welcomed_nodes), current_join_id);
}

static int
compare_int_fields(xmlNode *left, xmlNode *right, const char *field)
{
    const char *elem_l = crm_element_value(left, field);
    const char *elem_r = crm_element_value(right, field);

    int int_elem_l = crm_int_helper(elem_l, NULL);
    int int_elem_r = crm_int_helper(elem_r, NULL);

    if(int_elem_l < int_elem_r) {
	return -1;
	
    } else if(int_elem_l > int_elem_r) {
	return 1;
    }
	
    return 0;
}

/*	 A_DC_JOIN_PROCESS_REQ	*/
/*
	A_DC_JOIN_PROCESS_REQアクション処理(CRM_OP_JOIN_REQUESTをDCノードが受信した時の処理)
*/
void
do_dc_join_filter_offer(long long action,
	       enum crmd_fsa_cause cause,
	       enum crmd_fsa_state cur_state,
	       enum crmd_fsa_input current_input,
	       fsa_data_t *msg_data)
{
	xmlNode *generation = NULL;

	int cmp = 0;
	int join_id = -1;
	gboolean ack_nack_bool = TRUE;
	const char *ack_nack = CRMD_JOINSTATE_MEMBER;
	ha_msg_input_t *join_ack = fsa_typed_data(fsa_dt_ha_msg);

	const char *join_from = crm_element_value(join_ack->msg, F_CRM_HOST_FROM);
	const char *ref       = crm_element_value(join_ack->msg, XML_ATTR_REFERENCE);
	
	crm_node_t *join_node = crm_get_peer(0, join_from);

	crm_debug("Processing req from %s", join_from);
	
	generation = join_ack->xml;
	crm_element_value_int(join_ack->msg, F_CRM_JOIN_ID, &join_id);

	if(max_generation_xml != NULL && generation != NULL) {
	    int lpc = 0;
	    const char *attributes[] = {
		XML_ATTR_GENERATION_ADMIN,
		XML_ATTR_GENERATION,
		XML_ATTR_NUMUPDATES,
	    };
	    
	    for(lpc = 0; cmp == 0 && lpc < DIMOF(attributes); lpc++) {
			cmp = compare_int_fields(max_generation_xml, generation, attributes[lpc]);
	    }
	}
	
	if(join_id != current_join_id) {
		crm_debug("Invalid response from %s: join-%d vs. join-%d",
			  join_from, join_id, current_join_id);
		/* JOIN状態チェックを行う */
		check_join_state(cur_state, __FUNCTION__);
		return;
		
	} else if(join_node == NULL || crm_is_member_active(join_node) == FALSE) {
		crm_err("Node %s is not a member", join_from);
		ack_nack_bool = FALSE;
		
	} else if(generation == NULL) {
		crm_err("Generation was NULL");
		ack_nack_bool = FALSE;

	} else if(max_generation_xml == NULL) {
		max_generation_xml = copy_xml(generation);
		max_generation_from = crm_strdup(join_from);

	} else if(cmp < 0
		  || (cmp == 0 && safe_str_eq(join_from, fsa_our_uname))) {
		crm_debug("%s has a better generation number than"
			  " the current max %s",
			  join_from, max_generation_from);
		if(max_generation_xml) {
			crm_log_xml_debug(max_generation_xml, "Max generation");
		}
		crm_log_xml_debug(generation, "Their generation");
		
		crm_free(max_generation_from);
		free_xml(max_generation_xml);
		
		max_generation_from = crm_strdup(join_from);
		max_generation_xml = copy_xml(join_ack->xml);
	}
	
	if(ack_nack_bool == FALSE) {
		/* NACK this client */
		ack_nack = CRMD_JOINSTATE_NACK;
		crm_err("join-%d: NACK'ing node %s (ref %s)",
			join_id, join_from, ref);
	} else {
		crm_debug("join-%d: Welcoming node %s (ref %s)",
			  join_id, join_from, ref);
	}
	
	/* add them to our list of CRMD_STATE_ACTIVE nodes */
	/* integrated_nodesハッシュテーブルにCRM_OP_JOIN_REQUESTを送信して来たノードを追加する */
	g_hash_table_insert(
		integrated_nodes, crm_strdup(join_from), crm_strdup(ack_nack));

	/* integrated_nodesハッシュテーブルサイズと、CRM_OP_JOIN_REQUESTを送信して来たノードjoin_idをログ出力する */
	crm_debug("%u nodes have been integrated into join-%d",
		    g_hash_table_size(integrated_nodes), join_id);
	
	/* welcomed_nodesハッシュテーブルからCRM_OP_JOIN_REQUESTを送信して来たノードを削除する */
	g_hash_table_remove(welcomed_nodes, join_from);

	/* JOIN状態チェックを行う */
	if(check_join_state(cur_state, __FUNCTION__) == FALSE) {
		/* dont waste time by invoking the PE yet; */
		crm_debug("join-%d: Still waiting on %d outstanding offers",
			  join_id, g_hash_table_size(welcomed_nodes));
	}
}


/*	A_DC_JOIN_FINALIZE	*/
/*
	welcomed_nodesハッシュテーブルのサイズが０(すべての認識済みのノードからCRM_OP_JOIN_REQUESTを受信した)の場合
*/
void
do_dc_join_finalize(long long action,
		    enum crmd_fsa_cause cause,
		    enum crmd_fsa_state cur_state,
		    enum crmd_fsa_input current_input,
		    fsa_data_t *msg_data)
{
	char *sync_from = NULL;
	enum cib_errors rc = cib_ok;

	/* This we can do straight away and avoid clients timing us out
	 *  while we compute the latest CIB
	 */
	crm_debug("Finializing join-%d for %d clients",
		  current_join_id, g_hash_table_size(integrated_nodes));
	if(g_hash_table_size(integrated_nodes) == 0) {
		/* integrated_nodesハッシュテーブルにまだ、welcom_nodesハッシュテーブルからデータが１つも移送されていない場合 */
		/* は、I_ELECTION_DCへ戻る */
		/* 認識済みのメンバーから、どのノードからもCRM_OP_JOIN_REQUESTを受信し終わっていない状態ということになる */
	    /* If we don't even have ourself, start again */
	    register_fsa_error_adv(
		C_FSA_INTERNAL, I_ELECTION_DC, NULL, NULL, __FUNCTION__);
	    return;
	}
	
	clear_bit_inplace(fsa_input_register, R_HAVE_CIB);
	if(max_generation_from == NULL
	   || safe_str_eq(max_generation_from, fsa_our_uname)){
		set_bit_inplace(fsa_input_register, R_HAVE_CIB);
	}

	if(is_set(fsa_input_register, R_IN_TRANSITION)) {
		crm_warn("join-%d: We are still in a transition."
			 "  Delaying until the TE completes.", current_join_id);
		crmd_fsa_stall(NULL);
		return;
	}
	
	if(is_set(fsa_input_register, R_HAVE_CIB) == FALSE) {
		/* ask for the agreed best CIB */
		sync_from = crm_strdup(max_generation_from);
		crm_log_xml_debug(max_generation_xml, "Requesting version");
		set_bit_inplace(fsa_input_register, R_CIB_ASKED);

	} else {
		/* Send _our_ CIB out to everyone */
		sync_from = crm_strdup(fsa_our_uname);
	}

	crm_info("join-%d: Syncing the CIB from %s to the rest of the cluster",
		 current_join_id, sync_from);
	/* CIBにsync_from処理を実行する */
	rc = fsa_cib_conn->cmds->sync_from(
	    fsa_cib_conn, sync_from, NULL,cib_quorum_override);
	/* sync_from処理のコールバックをセットする */
	/* 		コールバック内からCRM_OP_JOIN_ACKNAKメッセージを送信する	*/
	fsa_cib_conn->cmds->register_callback(
		    fsa_cib_conn, rc, 60, FALSE, sync_from,
		    "finalize_sync_callback", finalize_sync_callback);
}
/*
	CIBへのsync_from処理のコールバック処理
		CRM_OP_JOIN_ACKNAKメッセージを送信する
*/
void
finalize_sync_callback(xmlNode *msg, int call_id, int rc,
		       xmlNode *output, void *user_data) 
{
	CRM_DEV_ASSERT(cib_not_master != rc);
	clear_bit_inplace(fsa_input_register, R_CIB_ASKED);
	if(rc != cib_ok) {
		do_crm_log((rc==cib_old_data?LOG_WARNING:LOG_ERR),
			      "Sync from %s resulted in an error: %s",
			      (char*)user_data, cib_error2string(rc));

		/* restart the whole join process */
		/* コールバックがcib_okではない場合は、I_ELECTION_DCに戻る */
		register_fsa_error_adv(
			C_FSA_INTERNAL, I_ELECTION_DC,NULL,NULL,__FUNCTION__);

	} else if(AM_I_DC && fsa_state == S_FINALIZE_JOIN) {
		/* 自ノードがDCノードで、fsa_stateがS_FINALIZE_JOINの場合 */
	    set_bit_inplace(fsa_input_register, R_HAVE_CIB);
	    clear_bit_inplace(fsa_input_register, R_CIB_ASKED);
	    
	    /* make sure dc_uuid is re-set to us */
	    /* JOIN状態チェック処理 */
	    if(check_join_state(fsa_state, __FUNCTION__) == FALSE) {
			crm_debug("Notifying %d clients of join-%d results",
			  g_hash_table_size(integrated_nodes), current_join_id);
			
			/*
				ノードをCIBのnodeエントリに追加して、CRM_OP_JOIN_ACKNAKメッセージを送信する
				また、クラスタメンバーとして認識したノードは、finalized_nodesハッシュテーブルに追加する	
				そして、integrated_nodesハッシュテーブルから削除する
			*/
			g_hash_table_foreach_remove(
		    	integrated_nodes, finalize_join_for, NULL);
	    }
		
	} else {
		crm_debug("No longer the DC in S_FINALIZE_JOIN: %s/%s",
			  AM_I_DC?"DC":"CRMd", fsa_state2string(fsa_state));
	}
	
	crm_free(user_data);
}

static void
join_update_complete_callback(xmlNode *msg, int call_id, int rc,
			      xmlNode *output, void *user_data)
{
	fsa_data_t *msg_data = NULL;
	
	if(rc == cib_ok) {
		crm_debug("Join update %d complete", call_id);
		check_join_state(fsa_state, __FUNCTION__);

	} else {
		crm_err("Join update %d failed", call_id);
		crm_log_xml(LOG_DEBUG, "failed", msg);
		register_fsa_error(C_FSA_INTERNAL, I_ERROR, NULL);
	}
}

/*	A_DC_JOIN_PROCESS_ACK	*/
/*
	CRM_OP_JOIN_ACKNACKを送信後、ノードからCRM_OP_JOIN_CONFIRMメッセージが送られて来たときの処理
*/
void
do_dc_join_ack(long long action,
	       enum crmd_fsa_cause cause,
	       enum crmd_fsa_state cur_state,
	       enum crmd_fsa_input current_input,
	       fsa_data_t *msg_data)
{
	int join_id = -1;
	int call_id = 0;
	ha_msg_input_t *join_ack = fsa_typed_data(fsa_dt_ha_msg);

	const char *join_id_s  = NULL;
	const char *join_state = NULL;
	const char *op         = crm_element_value(join_ack->msg, F_CRM_TASK);
	const char *join_from  = crm_element_value(join_ack->msg, F_CRM_HOST_FROM);

	if(safe_str_neq(op, CRM_OP_JOIN_CONFIRM)) {
		/* CRM_OP_JOIN_CONFIRMメッセージ以外のメッセージは無視してログを出力 */
		crm_debug("Ignoring op=%s message from %s", op, join_from);
		return;
	} 

	crm_element_value_int(join_ack->msg, F_CRM_JOIN_ID, &join_id);
	join_id_s = crm_element_value(join_ack->msg, F_CRM_JOIN_ID);

	/* now update them to "member" */
	
	crm_debug_2("Processing ack from %s", join_from);

	/* CRM_OP_JOIN_CONFIRMを送ってきたノードがfinalized_nodesハッシュテーブルに含まれるかサーチする */
	join_state = (const char *)
		g_hash_table_lookup(finalized_nodes, join_from);
	
	if(join_state == NULL) {
		/* 含まれていない場合は、無視 */
		crm_err("Join not in progress: ignoring join-%d from %s",
			join_id, join_from);
		return;
		
	} else if(safe_str_neq(join_state, CRMD_JOINSTATE_MEMBER)) {
		crm_err("Node %s wasnt invited to join the cluster",join_from);
		/* 含まれていて、CRMD_JOINSTATE_MEMBER状態の場合は、finalized_nodesハッシュテーブルからCRM_OP_JOIN_CONFIRMを送ってきたノードを削除 */
		g_hash_table_remove(finalized_nodes, join_from);
		return;
		
	} else if(join_id != current_join_id) {
		crm_err("Invalid response from %s: join-%d vs. join-%d",
			join_from, join_id, current_join_id);
		/* JOIN_IDが異なる場合も、finalized_nodesハッシュテーブルからCRM_OP_JOIN_CONFIRMを送ってきたノードを削除 */
		g_hash_table_remove(finalized_nodes, join_from);
		return;
	}
	
	/* 上記以外の場合も、finalized_nodesハッシュテーブルからCRM_OP_JOIN_CONFIRMを送ってきたノードを削除 */
	g_hash_table_remove(finalized_nodes, join_from);
	
	/* confirmed_nodesハッシュテーブルにRM_OP_JOIN_CONFIRMを送ってきたノードでサーチ */
	if(g_hash_table_lookup(confirmed_nodes, join_from) != NULL) {
		/* 既に登録済みの場合は、エラーメッセージを表示 */
		crm_err("join-%d: hash already contains confirmation from %s",
			join_id, join_from);
	}

	/* confirmed_nodesハッシュテーブルにCRM_OP_JOIN_CONFIRMを送ってきたノードを追加 */
	g_hash_table_insert(
		confirmed_nodes, crm_strdup(join_from), crm_strdup(join_id_s));

	/* join完了ログを出す */
 	crm_info("join-%d: Updating node state to %s for %s",
 		 join_id, CRMD_JOINSTATE_MEMBER, join_from);

	/* update CIB with the current LRM status from the node
	 * We dont need to notify the TE of these updates, a transition will
	 *   be started in due time
	 */
	/* CIBからCRM_OP_JOIN_CONFIRMを送ってきたノードのXML_CIB_TAG_LRM情報を消去 */
	erase_status_tag(join_from, XML_CIB_TAG_LRM, cib_scope_local);
	/* CIBを更新 */
	/* CRM_OP_JOIN_CONFIRMを送ってきたノードのXML_CIB_TAG_LRM情報も追加される */
	/* クラスタ構成ノードのLRMD情報が最新で更新 */
	fsa_cib_update(XML_CIB_TAG_STATUS, join_ack->xml,
		       cib_scope_local|cib_quorum_override|cib_can_create, call_id);
	/* CIBの更新コールバックをセット */
	add_cib_op_callback(
		fsa_cib_conn, call_id, FALSE, NULL, join_update_complete_callback);
 	crm_debug("join-%d: Registered callback for LRM update %d",
		  join_id, call_id);
}
/*
	ノードをCIBのnodeエントリに追加して、CRM_OP_JOIN_ACKNAKメッセージを送信する
	また、クラスタメンバーとして認識したノードは、finalized_nodesハッシュテーブルに追加する
*/
gboolean
finalize_join_for(gpointer key, gpointer value, gpointer user_data)
{
	const char *join_to = NULL;
	const char *join_state = NULL;
	xmlNode *acknak = NULL;
	crm_node_t *join_node = NULL;
	
	if(key == NULL || value == NULL) {
		return TRUE;
	}

	join_to    = (const char *)key;
	join_state = (const char *)value;

	/* make sure the node exists in the config section */
	/* CIBに対象ノードのnodeエントリを生成する */
	create_node_entry(join_to, join_to, NORMALNODE);

	join_node = crm_get_peer(0, join_to);
	if(crm_is_member_active(join_node) == FALSE) {
	    /*
	     * NACK'ing nodes that the membership layer doesn't know about yet
	     * simply creates more churn
	     *
	     * Better to leave them waiting and let the join restart when
	     * the new membership event comes in
	     *
	     * All other NACKs (due to versions etc) should still be processed
	     */
	    return TRUE;
	}
	
	/* send the ack/nack to the node */
	/* CRM_OP_JOIN_ACKNAKメッセージを生成する */
	acknak = create_request(
		CRM_OP_JOIN_ACKNAK, NULL, join_to,
		CRM_SYSTEM_CRMD, CRM_SYSTEM_DC, NULL);
	crm_xml_add_int(acknak, F_CRM_JOIN_ID, current_join_id);
	
	/* set the ack/nack */
	if(safe_str_eq(join_state, CRMD_JOINSTATE_MEMBER)) {
		/* 対象ノードの状態がCRMD_JOINSTATE_MEMBERの場合は、CRM_OP_JOIN_ACKNAKにXML_BOOLEAN_TRUEをセットする */
		crm_debug("join-%d: ACK'ing join request from %s, state %s",
			  current_join_id, join_to, join_state);
		crm_xml_add(acknak, CRM_OP_JOIN_ACKNAK, XML_BOOLEAN_TRUE);
		/* finalized_nodesハッシュテーブルにXML_BOOLEAN_TRUEを送信するノードをセットする */
		g_hash_table_insert(
			finalized_nodes,
			crm_strdup(join_to), crm_strdup(CRMD_JOINSTATE_MEMBER));
	} else {
		/* その他の状態の場合は、CRM_OP_JOIN_ACKNAKにXML_BOOLEAN_FALSEをセットする */
		crm_warn("join-%d: NACK'ing join request from %s, state %s",
			 current_join_id, join_to, join_state);
		
		crm_xml_add(acknak, CRM_OP_JOIN_ACKNAK, XML_BOOLEAN_FALSE);
	}
	/* セットしたCRM_OP_JOIN_ACKNAKメッセージを送信する */
	send_cluster_message(join_to, crm_msg_crmd, acknak, TRUE);
	/* 生成したメッセージを破棄する */
	free_xml(acknak);
	return TRUE;
}

void ghash_print_node(gpointer key, gpointer value, gpointer user_data);

/* 
	JOIN状態チェック処理
*/
gboolean
check_join_state(enum crmd_fsa_state cur_state, const char *source)
{
	crm_debug("Invoked by %s in state: %s",
		  source, fsa_state2string(cur_state));

	if(saved_ccm_membership_id != crm_peer_seq) {
		crm_info("%s: Membership changed since join started: %llu -> %llu",
			 source, saved_ccm_membership_id,
			 crm_peer_seq);
		register_fsa_input_before(C_FSA_INTERNAL, I_NODE_JOIN, NULL);
		
	} else if(cur_state == S_INTEGRATION) {
		/* 現在のstateがS_INTEGRATIONの時 */
		if(g_hash_table_size(welcomed_nodes) == 0) {
			/* welcomed_nodesハッシュテーブルのサイズが０(すべての認識済みのノードからCRM_OP_JOIN_REQUESTを受信した)の場合 */
			crm_debug("join-%d: Integration of %d peers complete: %s",
				  current_join_id,
				  g_hash_table_size(integrated_nodes), source);
			/* I_INTEGRATEDへ */
			register_fsa_input_before(
				C_FSA_INTERNAL, I_INTEGRATED, NULL);
			return TRUE;
		}

	} else if(cur_state == S_FINALIZE_JOIN) {
		/* 現在のstateがS_FINALIZE_JOINの時 */
		if(is_set(fsa_input_register, R_HAVE_CIB) == FALSE) {
			crm_debug("join-%d: Delaying I_FINALIZED until we have the CIB",
				  current_join_id);
			return TRUE;
			
		} else if(g_hash_table_size(integrated_nodes) == 0
			  && g_hash_table_size(finalized_nodes) == 0) {
			crm_debug("join-%d complete: %s",
				  current_join_id, source);
			/* I_FINALIZEDへ */
			register_fsa_input_later(C_FSA_INTERNAL, I_FINALIZED, NULL);
			
		} else if(g_hash_table_size(integrated_nodes) != 0
			  && g_hash_table_size(finalized_nodes) != 0) {
			char *msg = NULL;
			crm_err("join-%d: Waiting on %d integrated nodes"
				" AND %d finalized nodes",
				current_join_id,
				g_hash_table_size(integrated_nodes),
				g_hash_table_size(finalized_nodes));
			msg = crm_strdup("Integrated node");
			g_hash_table_foreach(integrated_nodes, ghash_print_node, msg);
			crm_free(msg);
			
			msg = crm_strdup("Finalized node");
			g_hash_table_foreach(finalized_nodes, ghash_print_node, msg);
			crm_free(msg);

		} else if(g_hash_table_size(integrated_nodes) != 0) {
			crm_debug("join-%d: Still waiting on %d integrated nodes",
				  current_join_id,
				  g_hash_table_size(integrated_nodes));
			
		} else if(g_hash_table_size(finalized_nodes) != 0) {
			crm_debug("join-%d: Still waiting on %d finalized nodes",
				  current_join_id,
				  g_hash_table_size(finalized_nodes));
		}
	}
	
	return FALSE;
}

void
do_dc_join_final(long long action,
		 enum crmd_fsa_cause cause,
		 enum crmd_fsa_state cur_state,
		 enum crmd_fsa_input current_input,
		 fsa_data_t *msg_data)
{
    crm_info("Ensuring DC, quorum and node attributes are up-to-date");
    update_attrd(NULL, NULL, NULL);
    crm_update_quorum(crm_have_quorum, TRUE);
}
