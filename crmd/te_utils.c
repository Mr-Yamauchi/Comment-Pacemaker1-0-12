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
#include <crm/cib.h>
#include <crm/msg_xml.h>
#include <crm/common/msg.h>
#include <crm/common/xml.h>
#include <tengine.h>
#include <crmd_fsa.h>
#include <crmd_messages.h>

GCHSource *stonith_src = NULL;
crm_trigger_t *stonith_reconnect = NULL;

static gboolean
fail_incompletable_stonith(crm_graph_t *graph) 
{
    const char *task = NULL;
    xmlNode *last_action = NULL;

    if(graph == NULL) {
	return FALSE;
    }
    
    slist_iter(
	synapse, synapse_t, graph->synapses, lpc,
	if (synapse->confirmed) {
	    continue;
	}

	slist_iter(
	    action, crm_action_t, synapse->actions, lpc,

	    if(action->type != action_type_crm || action->confirmed) {
		continue;
	    }

	    task = crm_element_value(action->xml, XML_LRM_ATTR_TASK);
	    if(task && safe_str_eq(task, CRM_OP_FENCE)) {
		action->failed = TRUE;
		last_action = action->xml;
		update_graph(graph, action);
		crm_notice("Failing action %d (%s): STONITHd terminated",
			   action->id, ID(action->xml));
	    }
	    );
	);

    if(last_action != NULL) {
	crm_warn("STONITHd failure resulted in un-runnable actions");
	abort_transition(INFINITY, tg_restart, "Stonith failure", last_action);
	return TRUE;
    }
	
    return FALSE;
}

static void
tengine_stonith_connection_destroy(gpointer user_data)
{
    if(stonith_src == NULL) {
	crm_info("Fencing daemon disconnected");

    } else {
	crm_crit("Fencing daemon connection failed");	
	mainloop_set_trigger(stonith_reconnect);
    }

    /* cbchan will be garbage at this point, arrange for it to be reset */
    set_stonithd_input_IPC_channel_NULL(); 
    stonith_src = NULL;

    fail_incompletable_stonith(transition_graph);
    trigger_graph();
    return;
}

static gboolean
tengine_stonith_dispatch(IPC_Channel *sender, void *user_data)
{
    while(stonithd_op_result_ready()) {
	if (sender->ch_status != IPC_CONNECT) {
	    /* The message which was pending for us is that
	     * the IPC status is now IPC_DISCONNECT */
	    break;
	}
	
	if(ST_FAIL == stonithd_receive_ops_result(FALSE)) {
	    crm_err("stonithd_receive_ops_result() failed");
	}
    }
    
    if (sender->ch_status != IPC_CONNECT) {
	tengine_stonith_connection_destroy(NULL);
	return FALSE;
    }
    return TRUE;
}

gboolean
te_connect_stonith(gpointer user_data)
{
	int lpc = 0;
	int rc = ST_OK;
	IPC_Channel *fence_ch = NULL;
	if(stonith_src != NULL) {
	    crm_debug_2("Still connected");
	    return TRUE;
	}
	
	for(lpc = 0; lpc < 30; lpc++) {
	    crm_info("Attempting connection to fencing daemon...");
	    
	    sleep(1);
	    rc = stonithd_signon("tengine");
	    if(rc == ST_OK) {
		break;
	    }
	    
	    if(user_data != NULL) {
		crm_err("Sign-in failed: triggered a retry");
		mainloop_set_trigger(stonith_reconnect);
		return TRUE;
	    }
	    
	    crm_err("Sign-in failed: pausing and trying again in 2s...");
	    sleep(1);
	}
	
	CRM_ASSERT(rc == ST_OK); /* If not, we failed 30 times... just get out */
	CRM_ASSERT(stonithd_set_stonith_ops_callback(
		       tengine_stonith_callback) == ST_OK);
	
	crm_debug_2("Grabbing IPC channel");
	fence_ch = stonithd_input_IPC_channel();
	CRM_ASSERT(fence_ch != NULL);
	
	crm_debug_2("Attaching to mainloop");
	stonith_src = G_main_add_IPC_Channel(
	    G_PRIORITY_LOW, fence_ch, FALSE, tengine_stonith_dispatch, NULL,
	    tengine_stonith_connection_destroy);
	
	CRM_ASSERT(stonith_src != NULL);
	crm_info("Connected");
	return TRUE;
}

gboolean
stop_te_timer(crm_action_timer_t *timer)
{
	const char *timer_desc = "action timer";
	
	if(timer == NULL) {
		return FALSE;
	}
	if(timer->reason == timeout_abort) {
		timer_desc = "global timer";
		crm_debug_2("Stopping %s", timer_desc);
	}
	
	if(timer->source_id != 0) {
		crm_debug_2("Stopping %s", timer_desc);
		g_source_remove(timer->source_id);
		timer->source_id = 0;

	} else {
		crm_debug_2("%s was already stopped", timer_desc);
		return FALSE;
	}

	return TRUE;
}

/*
	te_graph_triggerが叩かれ場合のトリガー処理
*/
gboolean
te_graph_trigger(gpointer user_data) 
{
    enum transition_status graph_rc = -1;
    if(transition_graph == NULL) {
		/* 実行グラフがない場合は処理しない */
		crm_debug("Nothing to do");
		return TRUE;
    }
    
    crm_debug_2("Invoking graph %d in state %s",
	      transition_graph->id, fsa_state2string(fsa_state));

    switch(fsa_state) {
	case S_STARTING:
	case S_PENDING:
	case S_NOT_DC:
	case S_HALT:
	case S_ILLEGAL:
	case S_STOPPING:
	case S_TERMINATE:
		/* 状態が実行できる状態でなければ処理しない */
	    return TRUE;
	    break;
	default:
	    break;
    }
    
    if(transition_graph->complete == FALSE) {
		graph_rc = run_graph(transition_graph);
		print_graph(LOG_DEBUG_3, transition_graph);

		if(graph_rc == transition_active) {
			crm_debug_3("Transition not yet complete");
			return TRUE;		

		} else if(graph_rc == transition_pending) {
			crm_debug_3("Transition not yet complete - no actions fired");
			return TRUE;		
		}
	
		if(graph_rc != transition_complete) {
			crm_err("Transition failed: %s", transition_status(graph_rc));
			print_graph(LOG_WARNING, transition_graph);
		}
    }
    
    crm_info("Transition %d is now complete", transition_graph->id);
    transition_graph->complete = TRUE;
    notify_crmd(transition_graph);
    
    return TRUE;	
}

void
trigger_graph_processing(const char *fn, int line) 
{
	/* transition_triggerトリガーを叩く */○変更前
	mainloop_set_trigger(transition_trigger);
	crm_debug_2("%s:%d - Triggered graph processing", fn, line);
}

void
abort_transition_graph(
	int abort_priority, enum transition_action abort_action,
	const char *abort_text, xmlNode *reason, const char *fn, int line) 
{
	int log_level = LOG_INFO;
	const char *magic = NULL;*/○変更前
	mainloop_set_trigger(transition_tr後g
	CRM_CHECK(transition_graph != NULL, eturn);
	
	if(reason) {
	    int diff_add_updates     = 0;
	    int diff_add_epoch       = 0;
	    int diff_add_admin_epoch = 0;
	    
	    int diff_del_updates     = 0;
	    int diff_del_epoch       = 0;
	    int diff_del_admin_epoch = 0;
	     int line) 
{
	/* 5ransition_triggerトリガーを叩く
	    xmlNode *diff = get_xpath_object("//"F_CIB_UPDATE_RESULT"//diff", reason, LOG_DEBUG_2);
	    magic = crm_element_value(reason, XML_ATTR_TRANSITION_MAGIC);

	    if(diff) {
		cib_diff_version_details(
		    diff,
		    &diff_add_admin_epoch, &diff_add_epoch, &diff_add_updates, 
		    &diff_del_admin_epoch, &diff_del_epoch, &diff_del_updates);
		if(crm_str_eq(TYPE(reason), XML_CIB_TAG_NVPAIR , TRUE)) {
		    /* name can be NULL in 1.0 */
		  char *magic = NULL;*/○変更前
	mainl(BAY2)   const char *name = NAME(reason);
		    const char *value = VALUE(reason);
		    do_crm_log(log_level,
		        "%s:%d - Triggered transition abort (complete=%d, tag=%s, id=%s, name=%s, value=%s, magic=%s, cib=%d.%d.%d) : %s",
		        fn, line, transition_graph->complete, TYPE(reason), ID(reason), name?name:"NA", value?value:"NA", magic?magic:"NA",
		        diff_add_admin_epoch,diff_add_epoch,diff_add_updates, abort_text);
		} else {
		    do_crm_log(log_level,
		        "%s:%d - Triggered transition abort (complete=%d, tag=%s, id=%s, magic=%s, cib=%d.%d.%d) : %s",
		        fn, line, transition_graph->complete, TYPE(reason), ID(reason), magic?magic:"NA",
		        diff_add_admin_epoch,diff_add_epoch,diff_add_updates, abort_text);
		}
		
	    } else {
		do_crm_log(log_level,
			   "%s:%d - Triggered transition abort (complete=%d, tag=%s, id=%s, magic=%s) : %s",
			   fn, line, transition_graph->complete, TYPE(reason), ID(reason), magic?magic:"NA", abort_text);
	    }

	} else {
	    do_crm_log(log_level,
		       "%s:%d - Triggered transition abort (complete=%d) : %s",
		       fn, line, transition_graph->complete, abort_text);
	}
	
	switch(fsa_(BAY5)state) {
	    case S_STARTING:
	    case S_PENDING:
	    case S_NOT_DC
	    g_level,
			   "%s:%d - 66igge6ed transition abort (complete=%d, tag=%s, id=%s, magic=%s) : %s",
			   fn, liNG:
			   NG:
	    case S_PENDIN
	    NG:
	    case S_PENDIN
	    NG:
	    case S_PENDIN
	    case S_PENDIN:
	    case S_HALT:
	    case S_ILLEGAL:
	    case S_STOPPING:
	    case S_TERMINATE:
		do_crm_log(log_level,
			   "Abort suppressed: state=%s (complete=%d)",
			   fsa_state2string(fsa_state), transition_graph->complete);
		return;
	    default:
		break;
	}

	if(magic == NULL && reason != NULL) {
	    crm_log_xml(log_level+1, "Cause", reason);
	}
	
	/* Make sure any queued calculations are discarded ASAP */
	crm_free(fsa_pe_ref);
	fsa_pe_ref = NULL;
	
	if(transition_graph->complete) {
	    if(transition_timer->period_ms > 0) {
		crm_timer_start(transition_timer);
	    } else {
		register_fsa_input(C_FSA_INTERNAL, I_PE_CALC, NULL);
	    }
	    return;
	}

	update_abort_priority(
		transition_graph, abort_priority, abort_action, abort_text);	
	
	mainloop_set_trigger(transition_trigger);
}

