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

#include <crm/cib.h>
#include <crm/msg_xml.h>
#include <crm/common/xml.h>
#include <crm/common/cluster.h>
#include <crm/crm.h>
#include <crmd_fsa.h>
#include <crmd_messages.h>
#include <crmd_callbacks.h>

GHashTable *voted = NULL;
uint highest_born_on = -1;
static int current_election_id = 1;

/*	A_ELECTION_VOTE	*/
/* 
	A_ELECTION_VOTEアクション
	CRM_OP_VOTEメッセージをクラスタの全ノードに送信する
*/
void
do_election_vote(long long action,
		 enum crmd_fsa_cause cause,
		 enum crmd_fsa_state cur_state,
		 enum crmd_fsa_input current_input,
		 fsa_data_t *msg_data)
{
	gboolean not_voting = FALSE;
	xmlNode *vote = NULL;
	
	/* don't vote if we're in one of these states or wanting to shut down */
	switch(cur_state) {
		case S_STARTING:
		case S_RECOVERY:
		case S_STOPPING:
		case S_TERMINATE:
			crm_warn("Not voting in election, we're in state %s",
				 fsa_state2string(cur_state));
			not_voting = TRUE;
			break;
		default:
			break;
	}

	if(not_voting == FALSE) {
		if(is_set(fsa_input_register, R_STARTING)) {
			not_voting = TRUE;
		}
	}	

	if(not_voting) {
		if(AM_I_DC) {
			register_fsa_input(C_FSA_INTERNAL, I_RELEASE_DC, NULL);

		} else {
			register_fsa_input(C_FSA_INTERNAL, I_PENDING, NULL);
		}
		/* メッセージを送信せずに抜ける */
		return;
	}
	/* CRM_OP_VOTEメッセージを作成する */
	vote = create_request(
		CRM_OP_VOTE, NULL, NULL,
		CRM_SYSTEM_CRMD, CRM_SYSTEM_CRMD, NULL);

	current_election_id++;
	crm_xml_add(vote, F_CRM_ELECTION_OWNER, fsa_our_uuid);
	crm_xml_add_int(vote, F_CRM_ELECTION_ID, current_election_id);

	/* CRM_OP_VOTEメッセージをクラスタの全ノードに送信する */
	send_cluster_message(NULL, crm_msg_crmd, vote, TRUE);
	free_xml(vote);

	crm_debug("Started election %d", current_election_id);
	if(voted) {
	    g_hash_table_destroy(voted);	    
	}
	voted = NULL;
	
	if(cur_state == S_ELECTION || cur_state == S_RELEASE_DC) {
		/* election_timeoutタイマーを仕掛ける */
		crm_timer_start(election_timeout);		

	} else if(cur_state != S_INTEGRATION) {
		crm_err("Broken? Voting in state %s",
			fsa_state2string(cur_state));
	}
	
	return;
}

char *dc_hb_msg = NULL;
int beat_num = 0;

gboolean
do_dc_heartbeat(gpointer data)
{
	return TRUE;
}

struct election_data_s 
{
		const char *winning_uname;
		unsigned int winning_bornon;
};

static void
log_member_uname(gpointer key, gpointer value, gpointer user_data)
{
    if(crm_is_member_active(value)) {
	crm_err("%s: %s", (char*)user_data, (char*)key);
    }
}

static void
log_node(gpointer key, gpointer value, gpointer user_data)
{
	crm_err("%s: %s", (char*)user_data, (char*)key);
}

/*
  A_ELECTION_CHECKアクション
  CRM_OP_VOTEメッセージを受信してから流れる処理
  
  voteハッシュテーブルに全てのアクティブなクラスタ構成メンバーからの受信をおこなったかどうかチェックし
  全ての受信が行われている場合は、I_ELECTION_DCへ遷移する */
*/
void
do_election_check(long long action,
		       enum crmd_fsa_cause cause,
		       enum crmd_fsa_state cur_state,
		       enum crmd_fsa_input current_input,
		  fsa_data_t *msg_data)
{
	int voted_size = 0;
	
	/* 現在のアクティブなメンバー数を取得する */
	int num_members = crm_active_members();

	if(voted) {
		/* votedハッシュテーブルが存在する場合は、テーブルサイズを取得する */
	    voted_size = g_hash_table_size(voted);
	}
	/* in the case of #voted > #members, it is better to
	 *   wait for the timeout and give the cluster time to
	 *   stabilize
	 */
	if(fsa_state != S_ELECTION) {
		/* S_ELECTION状態の場合はチェックしない */
		crm_debug("Ignore election check: we not in an election");

	} else if(voted_size >= num_members) {
		/* voteハッシュテーブルのサイズ（メンバー数)が現在のアクティブなメンバー数以上の場合 */
		/* we won and everyone has voted */
		
		/* メンバーが揃ったのでelection_timeoutタイマーを止める */
		crm_timer_stop(election_timeout);
		
		/* アクティブメンバー数とvoteサイズが一致もしくは、voteサイズが大きい状態になったので、 */
		/* メンバーは揃ったので、内部メッセージにI_ELECTION_DCをセットする */
		register_fsa_input(C_FSA_INTERNAL, I_ELECTION_DC, NULL);
		
		if(voted_size > num_members) {
			char *data = NULL;
			
			data = crm_strdup("member");
			g_hash_table_foreach(crm_peer_cache, log_member_uname, data);
			crm_free(data);
			
			data = crm_strdup("voted");
			g_hash_table_foreach(voted, log_node, data);
			crm_free(data);
			
		}
		crm_debug("Destroying voted hash");
		/* votedハッシュテーブルを破棄する */
		g_hash_table_destroy(voted);
		voted = NULL;
		
	} else {
		crm_debug("Still waiting on %d non-votes (%d total)",
			 num_members - voted_size, num_members);
	}

	return;
}

#define win_dampen  1  /* in seconds */
#define loss_dampen 2 /* in seconds */

/*	A_ELECTION_COUNT	*/
/*  CRM_OP_VOTE もしくは CRM_OP_NOVOTE メッセージを受信してから流れる処理 */
/*  CCMの認識しているbornからDCノードになれるかどうかも決定する */
/*  DCになれないノードは、CRM_OP_NOVOTEメッセージを送信する */

void
do_election_count_vote(long long action,
		       enum crmd_fsa_cause cause,
		       enum crmd_fsa_state cur_state,
		       enum crmd_fsa_input current_input,
		       fsa_data_t *msg_data)
{
	int election_id = -1;
	int log_level = LOG_INFO;
	gboolean done = FALSE;
	gboolean we_loose = FALSE;
	const char *op             = NULL;	
	const char *vote_from      = NULL;
	const char *your_version   = NULL;
	const char *election_owner = NULL;
	const char *reason	   = "unknown";
	crm_node_t *our_node = NULL, *your_node = NULL;
	ha_msg_input_t *vote = fsa_typed_data(fsa_dt_ha_msg);

	static time_t last_election_win = 0;
	static time_t last_election_loss = 0;
	
	/* if the membership copy is NULL we REALLY shouldnt be voting
	 * the question is how we managed to get here.
	 */
	
	CRM_CHECK(msg_data != NULL, return);
	CRM_CHECK(crm_peer_cache != NULL, return);
	CRM_CHECK(vote != NULL, crm_err("Bogus data from %s", msg_data->origin); return);
	CRM_CHECK(vote->msg != NULL, crm_err("Bogus data from %s", msg_data->origin); return);
	
	/* 受信メッセージデータを取り出す */
	op             = crm_element_value(vote->msg, F_CRM_TASK);
	vote_from      = crm_element_value(vote->msg, F_CRM_HOST_FROM);
	your_version   = crm_element_value(vote->msg, F_CRM_VERSION);
	election_owner = crm_element_value(vote->msg, F_CRM_ELECTION_OWNER);
	crm_element_value_int(vote->msg, F_CRM_ELECTION_ID, &election_id);

	CRM_CHECK(vote_from != NULL, vote_from = fsa_our_uname);
	
	/* CRM_OP_VOTEメッセージの送信元のノード情報を取得する */
	your_node = crm_get_peer(0, vote_from);
	/* 自ノードのノード情報を取得する */
	our_node = crm_get_peer(0, fsa_our_uname);
	
 	if(voted == NULL) {
		crm_debug("Created voted hash");
		/* votedハッシュテーブルが未作成の場合は作成する */
 		voted = g_hash_table_new_full(
			g_str_hash, g_str_equal,
			g_hash_destroy_str, g_hash_destroy_str);
 	}
	
	if(cur_state == S_STARTING) {
		/* 自ノードの状態が、まだ、S_STARTING状態の場合は、DCになれないのでCRM_OP_NOVOTEメッセージを送信する */
	    reason = "Still starting";
	    we_loose = TRUE;
	
	} else if(our_node == NULL || crm_is_member_active(our_node) == FALSE) {
		/* 自ノードがまだクラスタ構成として認識されていないか、アクティブでない場合は */
		/* DCになれないのでCRM_OP_NOVOTEメッセージを送信する */
	    reason = "We are not part of the cluster";
	    log_level = LOG_ERR;
	    we_loose = TRUE;

	} else if(your_node == NULL || crm_is_member_active(your_node) == FALSE) {
	    /* CRM_OP_VOTEメッセージの送信元のノードがクラスタ構成として認識されていないか、アクティブでない場合は */
	    /* ログのみを出力する */
	    reason = "Peer is not part of our cluster";
	    log_level = LOG_WARNING;
	    done = TRUE;

	} else if(election_id != current_election_id
	    && crm_str_eq(fsa_our_uuid, election_owner, TRUE)) {
		/* 現在のelection_idと受信したelection_idが違う場合も、ログのみ出力する */
	    log_level = LOG_DEBUG_2;
	    reason = "Superceeded";
	    done = TRUE;

	} else if(crm_str_eq(op, CRM_OP_NOVOTE, TRUE)) {
		/* DCになれないと思ったノードが送信したCRM_OP_NOVOTEメッセージの場合 */
	    char *op_copy = crm_strdup(op);
	    char *uname_copy = crm_strdup(vote_from);
	    CRM_ASSERT(crm_str_eq(fsa_our_uuid, election_owner, TRUE));
	    
	    /* update the list of nodes that have voted */
		/* votedハッシュテーブルにノードデータをセットする */
	    g_hash_table_replace(voted, uname_copy, op_copy);
	    reason = "Recorded";
	    done = TRUE;

	} else if(crm_str_eq(vote_from, fsa_our_uname, TRUE)) {
		/* 自ノードが送信したCRM_OP_VOTEメッセージを処理する場合 */
	    char *op_copy = crm_strdup(op);
	    char *uname_copy = crm_strdup(vote_from);
	    CRM_ASSERT(crm_str_eq(fsa_our_uuid, election_owner, TRUE));

	    /* update ourselves in the list of nodes that have voted */
		/* votedハッシュテーブルにノードデータをセットする */
	    g_hash_table_replace(voted, uname_copy, op_copy);
	    reason = "Recorded";
	    done = TRUE;
	    
	} else if(compare_version(your_version, CRM_FEATURE_SET) < 0) {
		/* 受信したCRM_OP_VOTEメッセージの送り元のversionがCRM_FEATURE_SETよりも小さい場合 */
	    /* 自ノードは、DCになれない */
	    reason = "Version";
	    we_loose = TRUE;
		
	} else if(compare_version(your_version, CRM_FEATURE_SET) > 0) {
		/* 受信したCRM_OP_VOTEメッセージの送り元のversionがCRM_FEATURE_SETよりも大きい場合、ログのみ出力する */
	    reason = "Version";
	    
	} else if(your_node->born < our_node->born) {
		/* 受信したCRM_OP_VOTEメッセージの送り元の方がbornが自ノードよりも小さい場合 */
	    reason = "Age";
	    /* 自ノードは、DCになれない */
	    we_loose = TRUE;
	    
	} else if(your_node->born > our_node->born) {
		/* 受信したCRM_OP_VOTEメッセージの送り元の方がbornが自ノードよりも大きい場合 */
	    /* 自ノードは、DCの候補 */
	    reason = "Age";

	} else if(fsa_our_uname == NULL) {
		/* 自ノードのノード名称がセットされていない場合 */
	    /* 自ノードは、DCになれない */
	    reason = "Unknown host name";
	    we_loose = TRUE;
	    
	} else if(strcasecmp(fsa_our_uname, vote_from) > 0) {
		/* 自ノードのノード名が送信元...*/
	    /* 自ノードは、DCになれない */
	    reason = "Host name";
	    we_loose = TRUE;
	    
	} else {
		/* その他の場合 */
	    reason = "Host name";
	    CRM_ASSERT(strcmp(fsa_our_uname, vote_from) != 0);
/* cant happen...
 *	} else if(strcasecmp(fsa_our_uname, vote_from) == 0) {
 *
 * default...
 *	} else { // strcasecmp(fsa_our_uname, vote_from) < 0
 *		we win
 */
	}

	if(done) {
	    do_crm_log(log_level+1, "Election %d (current: %d, owner: %s): Processed %s from %s (%s)",
		       election_id, current_election_id, election_owner, op, vote_from, reason);
	    
	} else if(we_loose) {
		/* born値の比較などから、DCノードになれないと判断した場合 */
		
		/* CRM_OP_NOVOTEメッセージをCRMD宛に生成する */
		xmlNode *novote = create_request(
			CRM_OP_NOVOTE, NULL, vote_from,
			CRM_SYSTEM_CRMD, CRM_SYSTEM_CRMD, NULL);

		do_crm_log(log_level+1, "Election %d (owner: %s) lost: %s from %s (%s)",
			   election_id, election_owner, op, vote_from, reason);
		
		/* DCノードにNULLをセットする */
		update_dc(NULL);
		
		/* election_timeoutタイマーを止める */
		crm_timer_stop(election_timeout);
		
		if(fsa_input_register & R_THE_DC) {
			crm_debug_3("Give up the DC to %s", vote_from);
			register_fsa_input(C_FSA_INTERNAL, I_RELEASE_DC, NULL);
			
		} else if(cur_state != S_STARTING) {
			crm_debug_3("We werent the DC anyway");
			register_fsa_input(C_FSA_INTERNAL, I_PENDING, NULL);
		}

		/* CRM_OP_NOVOTEメッセージのF_CRM_ELECTION_OWNERに受信メッセージのelection_ownerをセットする */
		crm_xml_add(novote, F_CRM_ELECTION_OWNER, election_owner);
		/* CRM_OP_NOVOTEメッセージのF_CRM_ELECTION_IDに受信メッセージのelection_idをセットする */
		crm_xml_add_int(novote, F_CRM_ELECTION_ID, election_id);
		
		/* CRM_OP_VOTEメッセージの送信元にCRM_OP_NOVOTEメッセージを送信する */
		send_cluster_message(vote_from, crm_msg_crmd, novote, TRUE);
		
		/* 送信メッセージを解放する */
		free_xml(novote);

		/* CIBのset_slave処理を実行する */
		fsa_cib_conn->cmds->set_slave(fsa_cib_conn, cib_scope_local);

		last_election_loss = time(NULL);
		last_election_win = 0;

	} else {
	    do_crm_log(log_level, "Election %d (owner: %s) pass: %s from %s (%s)",
		     election_id, election_owner, op, vote_from, reason);

	    if(last_election_loss) {
			time_t tm_now = time(NULL);
			if(tm_now - last_election_loss < (time_t)loss_dampen) {
		    	crm_info("Election %d ignore: We already lost an election less than %ds ago",
			      election_id, loss_dampen);
		    	update_dc(NULL);
		    return;
			}
			last_election_loss = 0;
	    }

#if 0
	    /* Enabling this code can lead to multiple DCs during SimulStart.
	     * Specifically when a node comes up after our last 'win' vote.
	     *
	     * Fixing and enabling this functionality might become important when
	     * we start running realy big clusters, but for now leave it disabled.
	     */
	    if(last_election_win) {
		time_t tm_now = time(NULL);
		if(tm_now - last_election_win < (time_t)win_dampen) {
		    crm_info("Election %d ignore: We already won an election less than %ds ago",
			      election_id, win_dampen);
		    return;
		}
	    }

	    last_election_win = time(NULL);
#endif
		/* I_ELECTIONへ */
	    register_fsa_input(C_FSA_INTERNAL, I_ELECTION, NULL);
	    g_hash_table_destroy(voted);
	    voted = NULL;
	}	
}

/*	A_ELECT_TIMER_START, A_ELECTION_TIMEOUT 	*/
/* we won */
void
do_election_timer_ctrl(long long action,
		    enum crmd_fsa_cause cause,
		    enum crmd_fsa_state cur_state,
		    enum crmd_fsa_input current_input,
		    fsa_data_t *msg_data)
{
}

/*
	CIB更新コールバック
*/
static void
feature_update_callback(xmlNode *msg, int call_id, int rc,
			xmlNode *output, void *user_data)
{
	if(rc != cib_ok) {
		fsa_data_t *msg_data = NULL;
		register_fsa_error(C_FSA_INTERNAL, I_ERROR, NULL);
	}
}

/*	 A_DC_TAKEOVER	*/
/*
	DCノード開始処理
*/
void
do_dc_takeover(long long action,
	       enum crmd_fsa_cause cause,
	       enum crmd_fsa_state cur_state,
	       enum crmd_fsa_input current_input,
	       fsa_data_t *msg_data)
{
	int rc = cib_ok;
	xmlNode *cib = NULL;
	static const char *cluster_type = NULL;
	
	if(cluster_type == NULL) {
	    cluster_type = getenv("HA_cluster_type");
	}
	if(cluster_type == NULL) {
	    cluster_type = "Heartbeat";
	}
	
	crm_info("Taking over DC status for this partition");
	/* -------- ★★★★★★★★フラグ的な意味あいでの、DCノードはここでセット --------- */
	set_bit_inplace(fsa_input_register, R_THE_DC);

#if SUPPORT_AIS
	if(is_openais_cluster()) {
	    send_ais_text(crm_class_quorum, NULL, TRUE, NULL, crm_msg_ais);
	}
#endif
	
	if(voted != NULL) {
		/* votedハッシュテーブルを破棄する */
		crm_debug_2("Destroying voted hash");
		g_hash_table_destroy(voted);
		voted = NULL;
	}
	
	/* R_JOIN_OK, R_INVOKE_PEフラグセット */
	set_bit_inplace(fsa_input_register, R_JOIN_OK);
	set_bit_inplace(fsa_input_register, R_INVOKE_PE);
	
	/* CIBのset_master処理を実行する */
	fsa_cib_conn->cmds->set_master(fsa_cib_conn, cib_scope_local);
	
	/* 初期CIBのXMLを生成する */
	cib = create_xml_node(NULL, XML_TAG_CIB);
	/* 生成したXMLにCRM_FEATURE_SETをセットする */
	crm_xml_add(cib, XML_ATTR_CRM_VERSION, CRM_FEATURE_SET);
	/* CIBを生成した初期XMLで更新する */
	fsa_cib_update(XML_TAG_CIB, cib, cib_quorum_override, rc);
	/* 更新コールバックをセットする */
	add_cib_op_callback(fsa_cib_conn, rc, FALSE, NULL, feature_update_callback);

	/* CIBの"dc-version","cluster-infrastructure"属性を更新する */
	update_attr(fsa_cib_conn, cib_none, XML_CIB_TAG_CRMCONFIG,
		    NULL, NULL, NULL, "dc-version", VERSION"-"BUILD_VERSION, FALSE);

	update_attr(fsa_cib_conn, cib_none, XML_CIB_TAG_CRMCONFIG,
		    NULL, NULL, NULL, "cluster-infrastructure", cluster_type, FALSE);
	
	/* config_readトリガーを叩く */
	mainloop_set_trigger(config_read);
	
	free_xml(cib);
}


/*	 A_DC_RELEASE	*/
/*
	DCノード状態の解放処理
*/
void
do_dc_release(long long action,
	      enum crmd_fsa_cause cause,
	      enum crmd_fsa_state cur_state,
	      enum crmd_fsa_input current_input,
	      fsa_data_t *msg_data)
{
	if(action & A_DC_RELEASE) {
		crm_debug("Releasing the role of DC");
		/* フラグのDCをクリア */
		clear_bit_inplace(fsa_input_register, R_THE_DC);
		
	} else if (action & A_DC_RELEASED) {
		crm_info("DC role released");
#if 0
		if( are there errors ) {
			/* we cant stay up if not healthy */
			/* or perhaps I_ERROR and go to S_RECOVER? */
			result = I_SHUTDOWN;
		}
#endif
		/* I_RELEASE_SUCCESSへ */
		register_fsa_input(C_FSA_INTERNAL, I_RELEASE_SUCCESS, NULL);
		
	} else {
		crm_err("Unknown action %s", fsa_action2string(action));
	}

	crm_debug_2("Am I still the DC? %s", AM_I_DC?XML_BOOLEAN_YES:XML_BOOLEAN_NO);

}

