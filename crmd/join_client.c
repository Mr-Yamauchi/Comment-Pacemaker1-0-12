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

#include <crmd_fsa.h>
#include <crmd_messages.h>


int reannounce_count = 0;
void join_query_callback(xmlNode *msg, int call_id, int rc,
			 xmlNode *output, void *user_data);

extern ha_msg_input_t *copy_ha_msg_input(ha_msg_input_t *orig);

/*	A_CL_JOIN_QUERY		*/
/* is there a DC out there? */
/*
	既に存在するDCノードがあるかも知れないので、CRM_OP_JOIN_ANNOUNCEメッセージを送信
*/
void
do_cl_join_query(long long action,
	    enum crmd_fsa_cause cause,
	    enum crmd_fsa_state cur_state,
	    enum crmd_fsa_input current_input,
		    fsa_data_t *msg_data)
{
	/* CRM_OP_JOIN_ANNOUNCEメッセージを生成する */
	xmlNode *req = create_request(CRM_OP_JOIN_ANNOUNCE, NULL, NULL,
					 CRM_SYSTEM_DC, CRM_SYSTEM_CRMD, NULL);

	sleep(1);  /* give the CCM time to propogate to the DC */
	
	/* DCノードをクリアする */
	update_dc(NULL); /* Unset any existing value so that the result is not discarded */
	crm_debug("Querying for a DC");
	/* CRM_OP_JOIN_ANNOUNCEメッセージを全クラスタメンバーに送信する */
	send_cluster_message(NULL, crm_msg_crmd, req, FALSE);
	free_xml(req);
}


/*	 A_CL_JOIN_ANNOUNCE	*/

/* this is kind of a workaround for the fact that we may not be around
 * or are otherwise unable to reply when the DC sends out A_WELCOME_ALL
 */
/*

	DCノード宛にCRM_OP_JOIN_ANNOUNCEメッセージを送信する
*/
void
do_cl_join_announce(long long action,
	    enum crmd_fsa_cause cause,
	    enum crmd_fsa_state cur_state,
	    enum crmd_fsa_input current_input,
	    fsa_data_t *msg_data)
{
	/* Once we hear from the DC, we can stop the timer
	 *
	 * This timer was started either on startup or when a node
	 * left the CCM list
	 */

	/* dont announce if we're in one of these states */
	if(cur_state != S_PENDING) {
		crm_warn("Do not announce ourselves in state %s",
			 fsa_state2string(cur_state));
		return;
	}

	if(AM_I_OPERATIONAL) {
		/* send as a broadcast */
		xmlNode *req = create_request(
			CRM_OP_JOIN_ANNOUNCE, NULL, NULL,
			CRM_SYSTEM_DC, CRM_SYSTEM_CRMD, NULL);

		crm_debug("Announcing availability");
		update_dc(NULL);
		send_cluster_message(NULL, crm_msg_crmd, req, FALSE);
		free_xml(req);
	
	} else {
		/* Delay announce until we have finished local startup */
		crm_warn("Delaying announce until local startup is complete");
		return;
	}
}


static int query_call_id = 0;

/*	 A_CL_JOIN_REQUEST	*/
/* aka. accept the welcome offer */
/* A_CL_JOIN_REQUESTアクション処理 */
/*     CRM_OP_JOIN_OFFERメッセージの受信処理 	*/
/*															*/
void
do_cl_join_offer_respond(long long action,
	    enum crmd_fsa_cause cause,
	    enum crmd_fsa_state cur_state,
	    enum crmd_fsa_input current_input,
	    fsa_data_t *msg_data)
{
	ha_msg_input_t *input = fsa_typed_data(fsa_dt_ha_msg);
	const char *welcome_from = crm_element_value(input->msg, F_CRM_HOST_FROM);
	const char *join_id = crm_element_value(input->msg, F_CRM_JOIN_ID);
	
#if 0
	if(we are sick) {
		log error ;

		/* save the request for later? */
		return;
	} 
#endif

	crm_debug_2("Accepting join offer: join-%s",
		    crm_element_value(input->msg, F_CRM_JOIN_ID));

	/* we only ever want the last one */
	if(query_call_id > 0) {
		crm_debug_3("Cancelling previous join query: %d", query_call_id);
		remove_cib_op_callback(query_call_id, FALSE);
		query_call_id = 0;
	}

	if(update_dc(input->msg) == FALSE) {
	    crm_warn("Discarding offer from %s (expected %s)", welcome_from, fsa_our_dc);
	    return;
	}

	CRM_DEV_ASSERT(input != NULL);
	/* CIBにQUERYを行う */
	query_call_id = fsa_cib_conn->cmds->query(
		fsa_cib_conn, NULL, NULL, cib_scope_local);
	/* コールバックをセットする */
	add_cib_op_callback(
	    fsa_cib_conn, query_call_id, FALSE, crm_strdup(join_id), join_query_callback);
	crm_debug_2("Registered join query callback: %d", query_call_id);

	/* fsa_actionにA_DC_TIMER_STOPアクションを追加して、fsa_sourceトリガーを叩いてcrmdに通知する */
	register_fsa_action(A_DC_TIMER_STOP);
}
/*
	CRM_OP_JOIN_OFFERメッセージ受信後のCIBへのクエリーコールバック処理
*/
void
join_query_callback(xmlNode *msg, int call_id, int rc,
		    xmlNode *output, void *user_data)
{
	xmlNode *local_cib = NULL;
	char *join_id = user_data;
	xmlNode *generation = create_xml_node(
		NULL, XML_CIB_TAG_GENERATION_TUPPLE);

	CRM_DEV_ASSERT(join_id != NULL);

	query_call_id = 0;
	
	if(rc == cib_ok) {
		local_cib = output;
		CRM_DEV_ASSERT(safe_str_eq(crm_element_name(local_cib), XML_TAG_CIB));
	}
	
	if(local_cib != NULL) {
		xmlNode *reply = NULL;
		crm_debug("Respond to join offer join-%s", join_id);
		crm_debug("Acknowledging %s as our DC", fsa_our_dc);
		copy_in_properties(generation, local_cib);

		/* DC宛のCRM_OP_JOIN_REQUESTメッセージを生成する */
		reply = create_request(
			CRM_OP_JOIN_REQUEST, generation, fsa_our_dc,
			CRM_SYSTEM_DC, CRM_SYSTEM_CRMD, NULL);

		crm_xml_add(reply, F_CRM_JOIN_ID, join_id);
		/* CRM_OP_JOIN_REQUESTメッセージをDCノード宛に送信する */
		send_cluster_message(fsa_our_dc, crm_msg_crmd, reply, TRUE);
		free_xml(reply);

	} else {
		crm_err("Could not retrieve Generation to attach to our"
			" join acknowledgement: %s", cib_error2string(rc));
		register_fsa_error_adv(
			C_FSA_INTERNAL, I_ERROR, NULL, NULL, __FUNCTION__);
	}
	
	crm_free(join_id);
	free_xml(generation);
}

/*	A_CL_JOIN_RESULT	*/
/* aka. this is notification that we have (or have not) been accepted */
/* 
	A_CL_JOIN_RESULTアクション処理
	CRM_OP_JOIN_ACKNAK もしくは CRM_OP_JOIN_CONFIRM メッセージの受信処理 
*/
void
do_cl_join_finalize_respond(long long action,
	    enum crmd_fsa_cause cause,
	    enum crmd_fsa_state cur_state,
	    enum crmd_fsa_input current_input,
	    fsa_data_t *msg_data)
{
	xmlNode *tmp1     = NULL;
	gboolean was_nack = TRUE;
	static gboolean first_join = TRUE;
	ha_msg_input_t *input = fsa_typed_data(fsa_dt_ha_msg);

	int join_id = -1;
	const char *op           = crm_element_value(input->msg,F_CRM_TASK);
	const char *ack_nack     = crm_element_value(input->msg,CRM_OP_JOIN_ACKNAK);
	const char *welcome_from = crm_element_value(input->msg,F_CRM_HOST_FROM);
	
	if(safe_str_neq(op, CRM_OP_JOIN_ACKNAK)) {
		/* CRM_OP_JOIN_CONFIRMメッセージの場合は、無視のログを出力して処理しない */
		crm_debug_2("Ignoring op=%s message", op);
		return;
	}

	/* calculate if it was an ack or a nack */
	/* 受信メッセージのACK/NACK値を判定する */
	if(crm_is_true(ack_nack)) {
		/* ACKなら、FALSE */
		was_nack = FALSE;
	}

	crm_element_value_int(input->msg, F_CRM_JOIN_ID, &join_id);
	
	if(was_nack) {
		/* CRM_OP_JOIN_ACKNAKメッセージでNACK応答を受信した場合は、クラスタに参加出来ない */
		crm_err("Join (join-%d) with leader %s failed (NACK'd): Shutting down",
			join_id, welcome_from);
		register_fsa_error(C_FSA_INTERNAL, I_ERROR, NULL);
		return;
	}

	if(AM_I_DC == FALSE && safe_str_eq(welcome_from, fsa_our_uname)) {
		crm_warn("Discarding our own welcome - we're no longer the DC");
		return;
	} 	
	
	/* DCノード情報を更新する */
	if(update_dc(input->msg) == FALSE) {
	    crm_warn("Discarding %s from %s (expected %s)", op, welcome_from, fsa_our_dc);
	    return;
	}

	/* send our status section to the DC */
	crm_debug("Confirming join join-%d: %s",
		  join_id, crm_element_value(input->msg, F_CRM_TASK));
	/* lrmdのリソース状態などから、自ノードのXMLデータ(XML_CIB_TAG_STATE("node_state")データ)を生成する */
	tmp1 = do_lrm_query(TRUE);
	if(tmp1 != NULL) {
		/* CRM_OP_JOIN_CONFIRMメッセージをDCノード宛に生成する */
		/* 生成したXMLデータも着いている */
		xmlNode *reply = create_request(
			CRM_OP_JOIN_CONFIRM, tmp1, fsa_our_dc,
			CRM_SYSTEM_DC, CRM_SYSTEM_CRMD, NULL);
		crm_xml_add_int(reply, F_CRM_JOIN_ID, join_id);
		
		crm_debug("join-%d: Join complete."
			  "  Sending local LRM status to %s",
			  join_id, fsa_our_dc);

		if(first_join) {		    
		    first_join = FALSE;

		    /*
		     * Clear any previous transient node attribute and lrm operations
		     *
		     * OpenAIS has a nasty habit of not being able to tell if a
		     *   node is returning or didn't leave in the first place.
		     * This confuses Pacemaker because it never gets a "node up"
		     *   event which is normally used to clean up the status section.
		     *
		     * Do not remove the resources though, they'll be cleaned up in
		     *   do_dc_join_ack().  Removing them here creates a race
		     *   condition if the crmd is being recovered.
		     * Instead of a list of active resources from the lrmd
		     *   we may end up with a blank status section.
		     * If we are _NOT_ lucky, we will probe for the "wrong" instance
		     *   of anonymous clones and end up with multiple active
		     *   instances on the machine.
		     */
		     
		     /* CIBの自ノードのtransient_attributesを削除する */
		    erase_status_tag(fsa_our_uname, XML_TAG_TRANSIENT_NODEATTRS, 0);

		    /* Just in case attrd was still around too */
		    if(is_not_set(fsa_input_register, R_SHUTDOWN)) {
				/* SHUTDOWNではない場合 */
				
				/* terminate属性、shutdown属性にNULLをセットする */
				update_attrd(fsa_our_uname, "terminate", NULL);
				update_attrd(fsa_our_uname, XML_CIB_ATTR_SHUTDOWN, NULL);
		    }
		}
		
		/* 生成したCRM_OP_JOIN_CONFIRMメッセージをDCノード宛に送信する */
		send_cluster_message(fsa_our_dc, crm_msg_crmd, reply, TRUE);
		free_xml(reply);
		
		if(AM_I_DC == FALSE) {
			/* 自ノードがDCノードでない場合は、I_NOT_DCへ */
 			register_fsa_input_adv(
			    cause, I_NOT_DC, NULL, A_NOTHING, TRUE, __FUNCTION__);
			update_attrd(NULL, NULL, NULL);
		}

		free_xml(tmp1);
		
	} else {
		crm_err("Could not send our LRM state to the DC");
		register_fsa_error(C_FSA_INTERNAL, I_FAIL, NULL);
	}
}
