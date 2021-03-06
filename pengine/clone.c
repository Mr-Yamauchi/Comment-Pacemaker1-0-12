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

#include <crm/msg_xml.h>
#include <allocate.h>
#include <utils.h>
#include <lib/pengine/utils.h>

#define VARIANT_CLONE 1
#include <lib/pengine/variant.h>

gint sort_clone_instance(gconstpointer a, gconstpointer b);

void child_stopping_constraints(
	clone_variant_data_t *clone_data, 
	resource_t *self, resource_t *child, resource_t *last,
	pe_working_set_t *data_set);

void child_starting_constraints(
	clone_variant_data_t *clone_data, 
	resource_t *self, resource_t *child, resource_t *last,
	pe_working_set_t *data_set);

static node_t *
parent_node_instance(const resource_t *rsc, node_t *node)
{
	node_t *ret = NULL;
	if(node != NULL) {
		ret = pe_find_node_id(
			rsc->parent->allowed_nodes, node->details->id);
	}
	return ret;
}

static gboolean did_fail(const resource_t *rsc)
{
    if(is_set(rsc->flags, pe_rsc_failed)) {
	return TRUE;
    }

    slist_iter(
	child_rsc, resource_t, rsc->children, lpc,
	if(did_fail(child_rsc)) {
	    return TRUE;
	}
	);
    return FALSE;
}


gint sort_clone_instance(gconstpointer a, gconstpointer b)
{
	int level = LOG_DEBUG_3;
	node_t *node1 = NULL;
	node_t *node2 = NULL;

	gboolean can1 = TRUE;
	gboolean can2 = TRUE;
	gboolean with_scores = TRUE;
	
	const resource_t *resource1 = (const resource_t*)a;
	const resource_t *resource2 = (const resource_t*)b;

	CRM_ASSERT(resource1 != NULL);
	CRM_ASSERT(resource2 != NULL);

	/* allocation order:
	 *  - active instances
	 *  - instances running on nodes with the least copies
	 *  - active instances on nodes that cant support them or are to be fenced
	 *  - failed instances
	 *  - inactive instances
	 */	

	if(resource1->running_on && resource2->running_on) {
		if(g_list_length(resource1->running_on) < g_list_length(resource2->running_on)) {
			do_crm_log_unlikely(level, "%s < %s: running_on", resource1->id, resource2->id);
			return -1;
			
		} else if(g_list_length(resource1->running_on) > g_list_length(resource2->running_on)) {
			do_crm_log_unlikely(level, "%s > %s: running_on", resource1->id, resource2->id);
			return 1;
		}
	}
	
	if(resource1->running_on) {
		node1 = resource1->running_on->data;
	}
	if(resource2->running_on) {
		node2 = resource2->running_on->data;
	}

	if(node1) {
	    node_t *match = pe_find_node_id(resource1->allowed_nodes, node1->details->id);
	    if(match == NULL || match->weight < 0) {
		do_crm_log_unlikely(level, "%s: current location is unavailable", resource1->id);
		node1 = NULL;
		can1 = FALSE;
	    }
	}

	if(node2) {
	    node_t *match = pe_find_node_id(resource2->allowed_nodes, node2->details->id);
	    if(match == NULL || match->weight < 0) {
		do_crm_log_unlikely(level, "%s: current location is unavailable", resource2->id);
		node2 = NULL;
		can2 = FALSE;
	    }
	}

	if(can1 != can2) {
		if(can1) {
			do_crm_log_unlikely(level, "%s < %s: availability of current location", resource1->id, resource2->id);
			return -1;
		}
		do_crm_log_unlikely(level, "%s > %s: availability of current location", resource1->id, resource2->id);
		return 1;
	}
	
	if(resource1->priority < resource2->priority) {
		do_crm_log_unlikely(level, "%s < %s: priority", resource1->id, resource2->id);
		return 1;

	} else if(resource1->priority > resource2->priority) {
		do_crm_log_unlikely(level, "%s > %s: priority", resource1->id, resource2->id);
		return -1;
	}
	
	if(node1 == NULL && node2 == NULL) {
			do_crm_log_unlikely(level, "%s == %s: not active",
					   resource1->id, resource2->id);
			return 0;
	}

	if(node1 != node2) {
		if(node1 == NULL) {
			do_crm_log_unlikely(level, "%s > %s: active", resource1->id, resource2->id);
			return 1;
		} else if(node2 == NULL) {
			do_crm_log_unlikely(level, "%s < %s: active", resource1->id, resource2->id);
			return -1;
		}
	}
	
	can1 = can_run_resources(node1);
	can2 = can_run_resources(node2);
	if(can1 != can2) {
		if(can1) {
			do_crm_log_unlikely(level, "%s < %s: can", resource1->id, resource2->id);
			return -1;
		}
		do_crm_log_unlikely(level, "%s > %s: can", resource1->id, resource2->id);
		return 1;
	}

	node1 = parent_node_instance(resource1, node1);
	node2 = parent_node_instance(resource2, node2);
	if(node1 != NULL && node2 == NULL) {
		do_crm_log_unlikely(level, "%s < %s: not allowed", resource1->id, resource2->id);
		return -1;
	} else if(node1 == NULL && node2 != NULL) {
		do_crm_log_unlikely(level, "%s > %s: not allowed", resource1->id, resource2->id);
		return 1;
	}
	
	if(node1 == NULL) {
		do_crm_log_unlikely(level, "%s == %s: not allowed", resource1->id, resource2->id);
		return 0;
	}

	if(node1->count < node2->count) {
		do_crm_log_unlikely(level, "%s < %s: count", resource1->id, resource2->id);
		return -1;

	} else if(node1->count > node2->count) {
		do_crm_log_unlikely(level, "%s > %s: count", resource1->id, resource2->id);
		return 1;
	}

	if(with_scores) {
	    int max = 0;
	    int lpc = 0;
	    GListPtr list1 = node_list_dup(resource1->allowed_nodes, FALSE, FALSE);
	    GListPtr list2 = node_list_dup(resource2->allowed_nodes, FALSE, FALSE);

	    /* Current score */
	    node1 = g_list_nth_data(resource1->running_on, 0);
	    node1 = pe_find_node_id(list1, node1->details->id);

	    node2 = g_list_nth_data(resource2->running_on, 0);
	    node2 = pe_find_node_id(list2, node2->details->id);

	    if(node1->weight < node2->weight) {
		do_crm_log_unlikely(level, "%s < %s: current score", resource1->id, resource2->id);
		return 1;

	    } else if(node1->weight > node2->weight) {
		do_crm_log_unlikely(level, "%s > %s: current score", resource1->id, resource2->id);
		return -1;
	    }

	    /* All scores */
	    list1 = g_list_sort(list1, sort_node_weight);
	    list2 = g_list_sort(list2, sort_node_weight);
	    max = g_list_length(list1);
	    if(max < g_list_length(list2)) {
		max = g_list_length(list2);
	    }
	    
	    for(;lpc < max; lpc++) {
		node1 = g_list_nth_data(list1, lpc);
		node2 = g_list_nth_data(list2, lpc);
		if(node1 == NULL) {
		    do_crm_log_unlikely(level, "%s < %s: node score NULL", resource1->id, resource2->id);
		    pe_free_shallow(list1); pe_free_shallow(list2);
		    return 1;
		} else if(node2 == NULL) {
		    do_crm_log_unlikely(level, "%s > %s: node score NULL", resource1->id, resource2->id);
		    pe_free_shallow(list1); pe_free_shallow(list2);
		    return -1;
		}
		
		if(node1->weight < node2->weight) {
		    do_crm_log_unlikely(level, "%s < %s: node score", resource1->id, resource2->id);
		    pe_free_shallow(list1); pe_free_shallow(list2);
		    return 1;
		    
		} else if(node1->weight > node2->weight) {
		    do_crm_log_unlikely(level, "%s > %s: node score", resource1->id, resource2->id);
		    pe_free_shallow(list1); pe_free_shallow(list2);
		    return -1;
		}
	    }

	    pe_free_shallow(list1); pe_free_shallow(list2);
	}

	can1 = did_fail(resource1);
	can2 = did_fail(resource2);
	if(can1 != can2) {
	    if(can1) {
		do_crm_log_unlikely(level, "%s > %s: failed", resource1->id, resource2->id);
		return 1;
	    }
	    do_crm_log_unlikely(level, "%s < %s: failed", resource1->id, resource2->id);
	    return -1;
	}

	if(node1 && node2) {
	    int max = 0;
	    int lpc = 0;
	    GListPtr list1 = g_list_append(NULL, node_copy(resource1->running_on->data));
	    GListPtr list2 = g_list_append(NULL, node_copy(resource2->running_on->data));

	    /* Possibly a replacement for the with_scores block above */
	    
	    slist_iter(
		constraint, rsc_colocation_t, resource1->parent->rsc_cons_lhs, lpc,
		do_crm_log_unlikely(level+1, "Applying %s to %s", constraint->id, resource1->id);
		
		list1 = rsc_merge_weights(
		    constraint->rsc_lh, resource1->id, list1,
		    constraint->node_attribute,
		    constraint->score/INFINITY, FALSE, TRUE);
		);    

	    slist_iter(
		constraint, rsc_colocation_t, resource2->parent->rsc_cons_lhs, lpc,
		do_crm_log_unlikely(level+1, "Applying %s to %s", constraint->id, resource2->id);
		
		list2 = rsc_merge_weights(
		    constraint->rsc_lh, resource2->id, list2,
		    constraint->node_attribute,
		    constraint->score/INFINITY, FALSE, TRUE);
		);    

	    list1 = g_list_sort(list1, sort_node_weight);
	    list2 = g_list_sort(list2, sort_node_weight);
	    max = g_list_length(list1);
	    if(max < g_list_length(list2)) {
		max = g_list_length(list2);
	    }
	    
	    for(;lpc < max; lpc++) {
		node1 = g_list_nth_data(list1, lpc);
		node2 = g_list_nth_data(list2, lpc);
		if(node1 == NULL) {
		    do_crm_log_unlikely(level, "%s < %s: colocated score NULL", resource1->id, resource2->id);
		    pe_free_shallow(list1); pe_free_shallow(list2);
		    return 1;
		} else if(node2 == NULL) {
		    do_crm_log_unlikely(level, "%s > %s: colocated score NULL", resource1->id, resource2->id);
		    pe_free_shallow(list1); pe_free_shallow(list2);
		    return -1;
		}
		
		if(node1->weight < node2->weight) {
		    do_crm_log_unlikely(level, "%s < %s: colocated score", resource1->id, resource2->id);
		    pe_free_shallow(list1); pe_free_shallow(list2);
		    return 1;
		    
		} else if(node1->weight > node2->weight) {
		    do_crm_log_unlikely(level, "%s > %s: colocated score", resource1->id, resource2->id);
		    pe_free_shallow(list1); pe_free_shallow(list2);
		    return -1;
		}
	    }

	    pe_free_shallow(list1); pe_free_shallow(list2);
	}
	
	
	do_crm_log_unlikely(level, "%s == %s: default %d", resource1->id, resource2->id, node2->weight);
	return 0;
}

static node_t *
can_run_instance(resource_t *rsc, node_t *node)
{
	node_t *local_node = NULL;
	clone_variant_data_t *clone_data = NULL;
	if(can_run_resources(node) == FALSE) {
		goto bail;

	} else if(is_set(rsc->flags, pe_rsc_orphan)) {
		goto bail;
	}

	local_node = parent_node_instance(rsc, node);
	get_clone_variant_data(clone_data, rsc->parent);

	if(local_node == NULL) {
		crm_warn("%s cannot run on %s: node not allowed",
			rsc->id, node->details->uname);
		goto bail;

	} else if(local_node->count < clone_data->clone_node_max) {
		return local_node;

	} else {
		crm_debug_2("%s cannot run on %s: node full",
			    rsc->id, node->details->uname);
	}

  bail:
	if(node) {
	    common_update_score(rsc, node->details->id, -INFINITY);
	}
	return NULL;
}


static node_t *
color_instance(resource_t *rsc, pe_working_set_t *data_set) 
{
	node_t *chosen = NULL;
	node_t *local_node = NULL;

	crm_debug_2("Processing %s", rsc->id);

	if(is_not_set(rsc->flags, pe_rsc_provisional)) {
		/* 配置決定済みの場合は、配置候補ノードのlocationを返す */
		return rsc->fns->location(rsc, NULL, FALSE);

	} else if(is_set(rsc->flags, pe_rsc_allocating)) {
		/* 既に配置処理が始まっている場合は、抜ける */
		crm_debug("Dependency loop detected involving %s", rsc->id);
		return NULL;
	}

	if(rsc->allowed_nodes) {
		slist_iter(try_node, node_t, rsc->allowed_nodes, lpc,
			   can_run_instance(rsc, try_node);
			);
	}

	/* 子リソースのcolorを処理する */
	chosen = rsc->cmds->color(rsc, data_set);
	if(chosen) {
		/* 子リソースの配置先が決まった場合、親cloneの配置候補にその配置先が含まれるか検索する */
		local_node = pe_find_node_id(
			rsc->parent->allowed_nodes, chosen->details->id);

		if(local_node) {
			/* 含まれる場合は、そのノードのcountをインクリメントする */
		    local_node->count++;
		} else if(is_set(rsc->flags, pe_rsc_managed)) {
			/* 含まれない場合は、エラーを出力する */
		    /* what to do? we can't enforce per-node limits in this case */
		    crm_config_err("%s not found in %s (list=%d)",
				   chosen->details->id, rsc->parent->id,
				   g_list_length(rsc->parent->allowed_nodes));
		}
	}

	return chosen;
}

/* 配置候補のノード数とclone_maxの判定(all)から */
/* clone親リソースのcolocation設定を子リソースにセットする */
static void append_parent_colocation(resource_t *rsc, resource_t *child, gboolean all) 
{
	
	/* cloneリソースがrsc指定されているcolocation情報を全て処理する */
    slist_iter(cons, rsc_colocation_t, rsc->rsc_cons, lpc,
	       if(all || cons->score < 0 || cons->score == INFINITY) {
			/* all=TRUEか、スコアが0未満か、INFINITYの場合は、rsc指定のcolocation情報を */
			/* 子リソースのcolocation情報にセットする */
		   child->rsc_cons = g_list_append(child->rsc_cons, cons);
	       }
	       
	);
	
	/* cloneリソースがwith-rsc指定されているcolocation情報を全て処理する */
    slist_iter(cons, rsc_colocation_t, rsc->rsc_cons_lhs, lpc,
	       if(all || cons->score < 0) {
			/* all=TRUEか、スコアが0未満の場合は、with-rsc指定のcolocation情報を */
			/* 子リソースのcolocation情報にセットする */
		   child->rsc_cons_lhs = g_list_append(child->rsc_cons_lhs, cons);
		   
		   
		   /* all=FALSEで呼ばれる、clone_maxよりも配置候補ノード数が大きい場合の数値およびINFINITYの */
		   /* colocation制約は、子リソースに展開されるので、子リソースのwith-rsc指定のcolocation情報 */
		   /* の処理時に影響されて動作に違いが出ることになる */
		   /* 例: colocation rsc="a" with-rsc="pingd" score=INIFINITY...
		          pingdのclone_max=1で、実起動ノード数が4などの場合に、aが故障するとpingdが再起動したりする */
	       }
	);
	
	
	/* この処理によって、cloneリソースが含まれるcolocation情報が */
	/* cloneリソースの子リソースにもセットされる */
	
}

/* cloneリソースのcolor処理 */
node_t *
clone_color(resource_t *rsc, pe_working_set_t *data_set)
{
	int allocated = 0;
	int available_nodes = 0;
	clone_variant_data_t *clone_data = NULL;
	
	/* cloneリソースのデータを取得する */
	get_clone_variant_data(clone_data, rsc);

	if(is_not_set(rsc->flags, pe_rsc_provisional)) {
		/* 配置決定済みの場合は、NULLを返す */
		return NULL;

	} else if(is_set(rsc->flags, pe_rsc_allocating)) {
		/* 既に配置処理が始まっている場合は、抜ける */
		crm_debug("Dependency loop detected involving %s", rsc->id);
		return NULL;
	}

	/* 配置処理の開始フラグをセットする */
	set_bit(rsc->flags, pe_rsc_allocating);
	crm_debug_2("Processing %s", rsc->id);

	/* this information is used by sort_clone_instance() when deciding in which 
	 * order to allocate clone instances
	 */

	/* cloneリソースがwith-rsc指定されているcolocation情報(rsc_cons_lhs)を全て処理する */
	/* with-rsc指定されていない場合は、ループしないので注意 */
	slist_iter(
	    constraint, rsc_colocation_t, rsc->rsc_cons_lhs, lpc,
	    
	    rsc->allowed_nodes = constraint->rsc_lh->cmds->merge_weights(
		constraint->rsc_lh, rsc->id, rsc->allowed_nodes,
		constraint->node_attribute, constraint->score/INFINITY, TRUE, TRUE);
		/* 上記によって、colocationのスコア値をrsc指定側の状態からこのリソース(with-rsc)配置候補ノードに影響させる */
		/* さらに内部の処理では、rsc指定側がwith-rsc指定されている分もたぐりがら、merge_weights()を繰り返して */
		/* colocationの関係性を反映させているので注意 */
	    );
	
	dump_node_scores(show_scores?0:scores_log_level, rsc, __FUNCTION__, rsc->allowed_nodes);
	
	/* count now tracks the number of clones currently allocated */
	/* cloneリソースの配置候補ノード情報のcountを０クリアする */
	slist_iter(node, node_t, rsc->allowed_nodes, lpc,
		   node->count = 0;
		);
	
	/* cloneリソースの全ての子リソースを処理する */
	slist_iter(child, resource_t, rsc->children, lpc,
		   if(g_list_length(child->running_on) > 0) {
				/* 子リソースが既に起動している場合(１以上の場合もあり) */
			   node_t *child_node = child->running_on->data;
			  	/* 子リソース起動しているノードの情報を取得して、countをアップする */
			   node_t *local_node = parent_node_instance(
				   child, child->running_on->data);
			   if(local_node) {
				   local_node->count++;
			   } else {
				   crm_err("%s is running on %s which isn't allowed",
					   child->id, child_node->details->uname);
			   }
		   }
		);
	
	/* cloneリソースの子リソースをソートしておく */
	rsc->children = g_list_sort(rsc->children, sort_clone_instance);

	/* count now tracks the number of clones we have allocated */
	/* cloneリソースの配置候補ノード情報のcountを０クリアする */
	slist_iter(node, node_t, rsc->allowed_nodes, lpc,
		   node->count = 0;
		);

	/* cloneリソースの配置候補ノード情報をweightでソートしておく */
	rsc->allowed_nodes = g_list_sort(
		rsc->allowed_nodes, sort_node_weight);

	/* cloneリソースの配置候補ノード情報で、リソースが起動可能なノード数をカウントしておく */
	slist_iter(node, node_t, rsc->allowed_nodes, lpc,
		   if(can_run_resources(node)) {
		       available_nodes++;
		   }
	    );

	/* cloneリソースの全ての子リソースを処理する */	
	slist_iter(child, resource_t, rsc->children, lpc,
		   if(allocated >= clone_data->clone_max) {
				/* 配置した子リソース数がcloneリソースのclone-maxを超えた場合は */
				/* その子リソースは配置不可のlocationをセットしておく */
			   crm_debug("Child %s not allocated - limit reached", child->id);
			   resource_location(child, NULL, -INFINITY, "clone_color:limit_reached", data_set);

		   } else if (clone_data->clone_max < available_nodes) {
		       /* Only include positive colocation preferences of dependant resources
			* if not every node will get a copy of the clone
			*/
				/* cloneリソースの配置可能なノード数がcloneリソースのclne_maxを超えた場合 */
		       append_parent_colocation(rsc, child, TRUE);

		   } else {
				/* cloneリソースの配置可能なノード数がcloneリソースのclne_maxを超えない場合 */
		       append_parent_colocation(rsc, child, FALSE);
		   }
		   
		   /* 子(instance)リソースのcolorを処理する */
		   if(color_instance(child, data_set)) {
				/* 配置数をカウントアップする */
			   allocated++;
		   }
		);

	crm_debug("Allocated %d %s instances of a possible %d",
		  allocated, rsc->id, clone_data->clone_max);

	/* 配置決定済みにセットする */
	clear_bit(rsc->flags, pe_rsc_provisional);

	/* 配置処理の開始フラグをクリアする */
	clear_bit(rsc->flags, pe_rsc_allocating);
	
	return NULL;
}

static void
clone_update_pseudo_status(
    resource_t *rsc, gboolean *stopping, gboolean *starting, gboolean *active) 
{
	if(rsc->children) {
	    slist_iter(child, resource_t, rsc->children, lpc,
		       clone_update_pseudo_status(child, stopping, starting, active)
		);
	    return;
	}
    
	CRM_ASSERT(active != NULL);
	CRM_ASSERT(starting != NULL);
	CRM_ASSERT(stopping != NULL);

	if(rsc->running_on) {
	    *active = TRUE;
	}
	
	slist_iter(
		action, action_t, rsc->actions, lpc,

		if(*starting && *stopping) {
			return;

		} else if(action->optional) {
			crm_debug_3("Skipping optional: %s", action->uuid);
			continue;

		} else if(action->pseudo == FALSE && action->runnable == FALSE){
			crm_debug_3("Skipping unrunnable: %s", action->uuid);
			continue;

		} else if(safe_str_eq(RSC_STOP, action->task)) {
			crm_debug_2("Stopping due to: %s", action->uuid);
			*stopping = TRUE;

		} else if(safe_str_eq(RSC_START, action->task)) {
			if(action->runnable == FALSE) {
				crm_debug_3("Skipping pseudo-op: %s run=%d, pseudo=%d",
					    action->uuid, action->runnable, action->pseudo);
			} else {
				crm_debug_2("Starting due to: %s", action->uuid);
				crm_debug_3("%s run=%d, pseudo=%d",
					    action->uuid, action->runnable, action->pseudo);
				*starting = TRUE;
			}
		}
		);

}

static action_t *
find_rsc_action(resource_t *rsc, const char *key, gboolean active_only, GListPtr *list)
{
    action_t *match = NULL;
    GListPtr possible = NULL;
    GListPtr active = NULL;
    possible = find_actions(rsc->actions, key, NULL);

    if(active_only) {
	slist_iter(op, action_t, possible, lpc,
		   if(op->optional == FALSE) {
		       active = g_list_append(active, op);
		   }
	    );
	
	if(active && g_list_length(active) == 1) {
	    match = g_list_nth_data(active, 0);
	}
	
	if(list) {
	    *list = active; active = NULL;
	}
	
    } else if(possible && g_list_length(possible) == 1) {
	match = g_list_nth_data(possible, 0);

    } if(list) {
	*list = possible; possible = NULL;
    }    

    if(possible) {
	g_list_free(possible);
    }
    if(active) {
	g_list_free(active);
    }
    
    return match;
}

static void
child_ordering_constraints(resource_t *rsc, pe_working_set_t *data_set)
{
    char *key = NULL;
    action_t *stop = NULL;
    action_t *start = NULL; 
    action_t *last_stop = NULL;
    action_t *last_start = NULL;
    gboolean active_only = TRUE; /* change to false to get the old behavior */
    clone_variant_data_t *clone_data = NULL;
    get_clone_variant_data(clone_data, rsc);

    if(clone_data->ordered == FALSE) {
	return;
    }
    
    slist_iter(
	child, resource_t, rsc->children, lpc,

	key = stop_key(child);
	stop = find_rsc_action(child, key, active_only, NULL);
	crm_free(key);
	
	key = start_key(child);
	start = find_rsc_action(child, key, active_only, NULL);
	crm_free(key);
	
	if(stop) {
	    if(last_stop) {
		/* child/child relative stop */
		order_actions(stop, last_stop, pe_order_implies_left);
	    }
	    last_stop = stop;
	}
	
	if(start) {
	    if(last_start) {
		/* child/child relative start */
		order_actions(last_start, start, pe_order_implies_left);
	    }
	    last_start = start;
	}
	);
}

void clone_create_actions(resource_t *rsc, pe_working_set_t *data_set)
{
	gboolean child_active = FALSE;
	gboolean child_starting = FALSE;
	gboolean child_stopping = FALSE;

	action_t *stop = NULL;
	action_t *stopped = NULL;

	action_t *start = NULL;
	action_t *started = NULL;

	resource_t *last_start_rsc = NULL;
	resource_t *last_stop_rsc = NULL;
	clone_variant_data_t *clone_data = NULL;

	get_clone_variant_data(clone_data, rsc);

	crm_debug_2("Creating actions for %s", rsc->id);
	
	slist_iter(
		child_rsc, resource_t, rsc->children, lpc,
		child_rsc->cmds->create_actions(child_rsc, data_set);
		clone_update_pseudo_status(
		    child_rsc, &child_stopping, &child_starting, &child_active);
		
		if(is_set(child_rsc->flags, pe_rsc_starting)) {
			last_start_rsc = child_rsc;
		}
		if(is_set(child_rsc->flags, pe_rsc_stopping)) {
			last_stop_rsc = child_rsc;
		}
		);

	/* start */
	start = start_action(rsc, NULL, !child_starting);
	started = custom_action(rsc, started_key(rsc),
				RSC_STARTED, NULL, !child_starting, TRUE, data_set);

	start->pseudo = TRUE;
	start->runnable = TRUE;
	started->pseudo = TRUE;
	started->priority = INFINITY;

	if(child_active || child_starting) {
	    started->runnable = TRUE;
	}
	
	child_ordering_constraints(rsc, data_set);
	child_starting_constraints(clone_data, rsc, NULL, last_start_rsc, data_set);
	if(clone_data->start_notify == NULL) {
	    clone_data->start_notify = create_notification_boundaries(rsc, RSC_START, start, started, data_set);
	}
	
	/* stop */
	stop = stop_action(rsc, NULL, !child_stopping);
	stopped = custom_action(rsc, stopped_key(rsc),
				RSC_STOPPED, NULL, !child_stopping, TRUE, data_set);

	stop->pseudo = TRUE;
	stop->runnable = TRUE;
	stopped->pseudo = TRUE;
	stopped->runnable = TRUE;
	stopped->priority = INFINITY;
	child_stopping_constraints(clone_data, rsc, NULL, last_stop_rsc, data_set);
	if(clone_data->stop_notify == NULL) {
	    clone_data->stop_notify = create_notification_boundaries(rsc, RSC_STOP, stop, stopped, data_set);

	    if(clone_data->stop_notify && clone_data->start_notify) {
		order_actions(clone_data->stop_notify->post_done, clone_data->start_notify->pre, pe_order_optional);	
	    }
	}
}

void
child_starting_constraints(
	clone_variant_data_t *clone_data,
	resource_t *rsc, resource_t *child, resource_t *last,
	pe_working_set_t *data_set)
{
	if(child == NULL && last == NULL) {
	    crm_debug("%s has no active children", rsc->id);
	    return;
	}
    
	if(child != NULL) {
		order_start_start(
		    rsc, child, pe_order_runnable_left|pe_order_implies_left_printed);
		
		new_rsc_order(child, RSC_START, rsc, RSC_STARTED, 
			      pe_order_implies_right_printed, data_set);
	}
	
	if(FALSE && clone_data->ordered) {
		if(child == NULL) {
		    /* last child start before global started */
		    new_rsc_order(last, RSC_START, rsc, RSC_STARTED, 
				  pe_order_runnable_left, data_set);

		} else if(last == NULL) {
			/* global start before first child start */
			order_start_start(
				rsc, child, pe_order_implies_left);

		} else {
			/* child/child relative start */
			order_start_start(last, child, pe_order_implies_left);
		}
	}
}

void
child_stopping_constraints(
	clone_variant_data_t *clone_data,
	resource_t *rsc, resource_t *child, resource_t *last,
	pe_working_set_t *data_set)
{
	if(child == NULL && last == NULL) {
	    crm_debug("%s has no active children", rsc->id);
	    return;
	}

	if(child != NULL) {
		order_stop_stop(rsc, child, pe_order_shutdown|pe_order_implies_left_printed);
		
		new_rsc_order(child, RSC_STOP, rsc, RSC_STOPPED,
			      pe_order_implies_right_printed, data_set);
	}
	
	if(FALSE && clone_data->ordered) {
		if(last == NULL) {
		    /* first child stop before global stopped */
		    new_rsc_order(child, RSC_STOP, rsc, RSC_STOPPED,
				  pe_order_runnable_left, data_set);
			
		} else if(child == NULL) {
			/* global stop before last child stop */
			order_stop_stop(
				rsc, last, pe_order_implies_left);
		} else {
			/* child/child relative stop */
			order_stop_stop(child, last, pe_order_implies_left);
		}
	}
}


void
clone_internal_constraints(resource_t *rsc, pe_working_set_t *data_set)
{
	resource_t *last_rsc = NULL;	
	clone_variant_data_t *clone_data = NULL;
	get_clone_variant_data(clone_data, rsc);

	native_internal_constraints(rsc, data_set);
	
	/* global stop before stopped */
	new_rsc_order(rsc, RSC_STOP, rsc, RSC_STOPPED, pe_order_runnable_left, data_set);

	/* global start before started */
	new_rsc_order(rsc, RSC_START, rsc, RSC_STARTED, pe_order_runnable_left, data_set);
	
	/* global stopped before start */
	new_rsc_order(rsc, RSC_STOPPED, rsc, RSC_START, pe_order_optional, data_set);
	
	slist_iter(
		child_rsc, resource_t, rsc->children, lpc,

		child_rsc->cmds->internal_constraints(child_rsc, data_set);

		child_starting_constraints(
			clone_data, rsc, child_rsc, last_rsc, data_set);

		child_stopping_constraints(
			clone_data, rsc, child_rsc, last_rsc, data_set);

		last_rsc = child_rsc;
		);
}

static void
assign_node(resource_t *rsc, node_t *node, gboolean force)
{
    if(rsc->children) {
	slist_iter(
		child_rsc, resource_t, rsc->children, lpc,
		native_assign_node(child_rsc, NULL, node, force);
	    );
	return;
    }
    native_assign_node(rsc, NULL, node, force);
}

static resource_t*
find_compatible_child_by_node(
    resource_t *local_child, node_t *local_node, resource_t *rsc, enum rsc_role_e filter, gboolean current)
{
	node_t *node = NULL;
	clone_variant_data_t *clone_data = NULL;
	get_clone_variant_data(clone_data, rsc);
	
	if(local_node == NULL) {
		crm_err("Can't colocate unrunnable child %s with %s",
			 local_child->id, rsc->id);
		return NULL;
	}
	
	slist_iter(
		child_rsc, resource_t, rsc->children, lpc,

		/* enum rsc_role_e next_role = minimum_resource_state(child_rsc, current); */
		enum rsc_role_e next_role = child_rsc->fns->state(child_rsc, current);
		node = child_rsc->fns->location(child_rsc, NULL, current);

		if(filter != RSC_ROLE_UNKNOWN && next_role != filter) {
		    crm_debug_3("Filtered %s", child_rsc->id);
		    continue;
		}
		
		if(node && local_node && node->details == local_node->details) {
			crm_debug_2("Pairing %s with %s on %s",
				    local_child->id, child_rsc->id, node->details->uname);
			return child_rsc;
		}
		);

	crm_debug_3("Can't pair %s with %s", local_child->id, rsc->id);
	return NULL;
}

resource_t*
find_compatible_child(
    resource_t *local_child, resource_t *rsc, enum rsc_role_e filter, gboolean current)
{
	resource_t *pair = NULL;
	GListPtr scratch = NULL;
	node_t *local_node = NULL;
	clone_variant_data_t *clone_data = NULL;
	/* with-rsc指定リソースのclone情報を取得する */
	get_clone_variant_data(clone_data, rsc);
	
	/* rsc指定リソースの１ノードのみの配置先ノード(allocated_to)を検索する */
	/* native_location()を呼び出している */
	local_node = local_child->fns->location(local_child, NULL, current);
	if(local_node) {
		/* rsc指定リソースの１ノードのみの配置先が見つかった場合 */
		/* そのノードでwith-rscリソースの配置先があるか検索した結果を返す */
	    return find_compatible_child_by_node(local_child, local_node, rsc, filter, current);
	}

	/* 見つからない場合は、rscリソースの配置候補ノード情報を複製する */
	scratch = node_list_dup(local_child->allowed_nodes, FALSE, TRUE);
	scratch = g_list_sort(scratch, sort_node_weight);

	/* 全てのrscリソースの配置候補ノードを処理する */
	slist_iter(
		node, node_t, scratch, lpc,

		/* rscリソースの配置候補ノードの１つで、with-rscリソースの配置先があるか検索する */
		pair = find_compatible_child_by_node(
		    local_child, node, rsc, filter, current);
		if(pair) {
			/* あった場合は終了 */
		    goto done;
		}
		);
	
	crm_debug("Can't pair %s with %s", local_child->id, rsc->id);
  done:
	slist_destroy(node_t, node, scratch, crm_free(node));
	/* 検索した配置候補ノードの結果を返す */
	return pair;
}

/* cloneリソースのcolocationのrsc指定を処理する(実実行処理) */
/* コメントにもあるとおり、現状、cloneではchildとして、colocationのrsc指定は、primitive単位で処理される */
/* よってこの処理は実行されていない */
void clone_rsc_colocation_lh(
	resource_t *rsc_lh, resource_t *rsc_rh, rsc_colocation_t *constraint)
{
	/* -- Never called --
	 *
	 * Instead we add the colocation constraints to the child and call from there
	 */
	
	CRM_CHECK(FALSE, crm_err("This functionality is not thought to be used. Please report a bug."));
	CRM_CHECK(rsc_lh, return);
	CRM_CHECK(rsc_rh, return);
	
	slist_iter(
		child_rsc, resource_t, rsc_lh->children, lpc,
		
		child_rsc->cmds->rsc_colocation_lh(child_rsc, rsc_rh, constraint);
		);

	return;
}

/* cloneリソースのcolocationのwith-rsc指定を処理する */
void clone_rsc_colocation_rh(
	resource_t *rsc_lh, resource_t *rsc_rh, rsc_colocation_t *constraint)
{
	gboolean do_interleave = FALSE;
	clone_variant_data_t *clone_data = NULL;
	clone_variant_data_t *clone_data_lh = NULL;

	CRM_CHECK(rsc_lh != NULL, return);
	CRM_CHECK(rsc_lh->variant == pe_native, return);
	
	/* with-rsc指定リソースのcloneデータを取得する */
	get_clone_variant_data(clone_data, constraint->rsc_rh);
	crm_debug_3("Processing constraint %s: %s -> %s %d",
		    constraint->id, rsc_lh->id, rsc_rh->id, constraint->score);

	if(constraint->rsc_lh->variant >= pe_clone) {
		/* colocationのrsc指定リソースがcloneかmasterの場合 */
	    
		/* rsc指定リソースのcloneデータを取得する */
	    get_clone_variant_data(clone_data_lh, constraint->rsc_lh);
	    
	    if(clone_data->clone_node_max != clone_data_lh->clone_node_max) {
			
			/* rsc指定のリソースのclone_node_maxとwith-rsc指定のclone_node_maxが一致しない場合は */
			/* 処理出来ないのでエラーを出す */
			
			crm_config_err("Cannot interleave "XML_CIB_TAG_INCARNATION
			       " %s and %s because"
			       " they do not support the same number of"
			       " resources per node",
			       constraint->rsc_lh->id, constraint->rsc_rh->id);
			
		/* only the LHS side needs to be labeled as interleave */
	    } else if(clone_data_lh->interleave) {
			/* rsc指定リソースのinterleave指定がtrueの場合は、フラグをセットする */
			do_interleave = TRUE;
	    }
	}

	if(rsc_rh == NULL) {
		pe_err("rsc_rh was NULL for %s", constraint->id);
		return;
		
	} else if(is_set(rsc_rh->flags, pe_rsc_provisional)) {
		crm_debug_3("%s is still provisional", rsc_rh->id);
		return;

	} else if(do_interleave) {
		/* rsc指定リソースのinterleaveがTRUEの場合 */
		
	    resource_t *rh_child = NULL;
		
		/* colocation指定されてるrsc指定リソースと、with-rsc指定リソースが同一ノードで */
		/* 配置候補にあるかどうかを検索する */
	    rh_child = find_compatible_child(rsc_lh, rsc_rh, RSC_ROLE_UNKNOWN, FALSE);
	    
	    if(rh_child) {
			/* with-rsc指定リソースが同一ノードで配置候補にある場合は、rsc指定リソースの */
			/* rsc_location_lh()を呼び出して、colocationを処理する */
			crm_debug("Pairing %s with %s", rsc_lh->id, rh_child->id);
			rsc_lh->cmds->rsc_colocation_lh(rsc_lh, rh_child, constraint);

	    } else if(constraint->score >= INFINITY) {
			/* with-rsc指定リソースがまだ起動可能でない場合でcolocationのスコアがINFNINITYお場合は、*/
			/* rscリソースは配置不可にする */
			crm_notice("Cannot pair %s with instance of %s", rsc_lh->id, rsc_rh->id);
			assign_node(rsc_lh, NULL, TRUE);

	    } else {
			crm_debug("Cannot pair %s with instance of %s", rsc_lh->id, rsc_rh->id);
	    }
	    
	    return;
	    
	} else if(constraint->score >= INFINITY) {
		/* 通常（rsc指定リソースのinterleaveがFALSEの場合）で、colocationのスコアがINFINITYの場合 */

		GListPtr rhs = NULL;
		
		/* with-rscリソースの全ての子リソースを処理する */
		slist_iter(
			child_rsc, resource_t, rsc_rh->children, lpc,
			/* 子リソースのlocationを検索する */
			node_t *chosen = child_rsc->fns->location(child_rsc, NULL, FALSE);
			if(chosen != NULL) {
				/* 配置候補がある場合は、リストに追加する */
				rhs = g_list_append(rhs, chosen);
			}
			/* この処理で、with-rsc指定されたリソースが配置候補として動けるのかどうかのリストが出来る */
			);
		
		/* with-rscリソースの配置候補をrsc指定の配置候補へ反映する */
		/* これによって、withリソースの配置候補で、with-rsc指定されたリソースが配置候補にない場合 */
		/* (with-rscリソースが起動出来ないノード)には、rscリソース自体も配置できなく(-INFININITYがセット)される */
		rsc_lh->allowed_nodes = node_list_exclude(rsc_lh->allowed_nodes, rhs, FALSE);
		g_list_free(rhs);
		return;
	}

	/* 上記以外(スコアが数値指定の場合)は、 */
	/* with-rsc指定リソースの全ての子リソースについても、colocationを処理する */
	/* これによって、数値指定の場合は、cloneリソースの子リソース全てのwith-rsc指定として処理される */
	slist_iter(
		child_rsc, resource_t, rsc_rh->children, lpc,
		
		child_rsc->cmds->rsc_colocation_rh(rsc_lh, child_rsc, constraint);
		);
}

/*

  Clone <-> Clone ordering
  
  S  : Start(ed)
  S' : Stop(ped)
  P  : Promote(d)
  D  : Demote(d)
  
  Started == Demoted

       First A then B
    A:0		    B:0
 Old	New	Old	New

 S'	S'	S	S'
 S'	S'	S'	-
 S'	S	S	S+
 S'	S	S'	S
 S	S'	S	S'
 S	S'	S'	-
 S	S	S	-
 S	S	S'	S

 S'	S'	P	S'
 S'	S'	S'	-
 S'	P	P	P+
 S'	P	S'	P
 P	S'	P	S'
 P	S'	S'	-
 P	P	P	-
 P	P	S'	P

 D	D	P	D
 D	D	D	-
 D	P	P	P+
 D	P	D	P
 P	D	P	D
 P	D	D	-
 P	P	P	-
 P	P	D	P

  Clone <-> Primitive ordering
  
  S  : Start(ed)
  S' : Stop(ped)
  P  : Promote(d)
  D  : Demote(d)
  F  : False
  T  : True
  F' : A good idea?
  
  Started == Demoted

       First A then B
    A:0		    B
 Old	New	Old	Create Constraint

 S'	S'	S	F
 S'	S'	S'	F'
 S	S'	S	T
 S	S'	S'	F
 S'	S	S	T
 S'	S	S'	T
 S	S	S	F'
 S	S	S'	T

 S'	S'	S	F
 S'	S'	S'	F'
 P	S'	S	T
 P	S'	S'	F
 S'	P	S	T
 S'	P	S'	T
 P	P	S	F'
 P	P	S'	F

 S'	S'	S	F
 S'	S'	S'	F'
 D	S'	S	T
 D	S'	S'	F
 S'	D	S	T
 S'	D	S'	T
 D	D	S	F'
 D	D	S'	T
 
*/
static gboolean detect_restart(resource_t *rsc) 
{
    gboolean restart = FALSE;
    
    /* Look for restarts */
    action_t *start = NULL;
    char *key = start_key(rsc);
    GListPtr possible_matches = find_actions(rsc->actions, key, NULL);
    crm_free(key);
		
    if(possible_matches) {
	start = possible_matches->data;
	g_list_free(possible_matches);
    }
		
    if(start != NULL && start->optional == FALSE) {
	restart = TRUE;
	crm_debug_2("Detected a restart for %s", rsc->id);
    }

    /* Otherwise, look for moves */
    if(restart == FALSE) {
	GListPtr old_hosts = NULL;
	GListPtr new_hosts = NULL;
	GListPtr intersection = NULL;

	rsc->fns->location(rsc, &old_hosts, TRUE);
	rsc->fns->location(rsc, &new_hosts, FALSE);
	intersection = node_list_and(old_hosts, new_hosts, FALSE);

	if(intersection == NULL) {
	    restart = TRUE; /* Actually a move but the result is the same */
	    crm_debug_2("Detected a move for %s", rsc->id);
	}

	slist_destroy(node_t, node, intersection, crm_free(node));
	g_list_free(old_hosts);
	g_list_free(new_hosts);
    }

    return restart;
}

static void clone_rsc_order_lh_non_clone(resource_t *rsc, order_constraint_t *order, pe_working_set_t *data_set)
{
    GListPtr hosts = NULL;
    GListPtr rh_hosts = NULL;
    GListPtr intersection = NULL;

    const char *reason = "unknown";
    enum action_tasks task = start_rsc;
    enum rsc_role_e lh_role = RSC_ROLE_STARTED;

    int any_ordered = 0;
    gboolean down_stack = TRUE;
		    
    crm_debug_2("Clone-to-* ordering: %s -> %s 0x%.6x",
		order->lh_action_task, order->rh_action_task, order->type);
		    
    if(strstr(order->rh_action_task, "_"RSC_STOP"_0")
       || strstr(order->rh_action_task, "_"RSC_STOPPED"_0")) {
	task = stop_rsc;
	reason = "down activity";
	lh_role = RSC_ROLE_STOPPED;
	order->rh_rsc->fns->location(order->rh_rsc, &rh_hosts, down_stack);
			
    } else if(strstr(order->rh_action_task, "_"RSC_DEMOTE"_0")
	      || strstr(order->rh_action_task, "_"RSC_DEMOTED"_0")) {
	task = action_demote;
	reason = "demotion activity";
	lh_role = RSC_ROLE_SLAVE;
	order->rh_rsc->fns->location(order->rh_rsc, &rh_hosts, down_stack);
			
    } else if(strstr(order->lh_action_task, "_"RSC_PROMOTE"_0")
	      || strstr(order->lh_action_task, "_"RSC_PROMOTED"_0")) {
	task = action_promote;
	down_stack = FALSE;
	reason = "promote activity";
	order->rh_rsc->fns->location(order->rh_rsc, &rh_hosts, down_stack);
	lh_role = RSC_ROLE_MASTER;
			
    } else if(strstr(order->rh_action_task, "_"RSC_START"_0")
	      || strstr(order->rh_action_task, "_"RSC_STARTED"_0")) {
	task = start_rsc;
	down_stack = FALSE;
	reason = "up activity";
	order->rh_rsc->fns->location(order->rh_rsc, &rh_hosts, down_stack);
	/* if(order->rh_rsc->variant > pe_clone) { */
	/*     lh_role = RSC_ROLE_SLAVE; */
	/* } */

    } else {
	crm_err("Unknown task: %s", order->rh_action_task);
	return;
    }
		    
    /* slist_iter(h, node_t, rh_hosts, llpc, crm_info("RHH: %s", h->details->uname)); */

    slist_iter(
	child_rsc, resource_t, rsc->children, lpc,

	gboolean create = FALSE;
	gboolean restart = FALSE;
	enum rsc_role_e lh_role_new = child_rsc->fns->state(child_rsc, FALSE);
	enum rsc_role_e lh_role_old = child_rsc->fns->state(child_rsc, TRUE);
	enum rsc_role_e child_role = child_rsc->fns->state(child_rsc, down_stack);

	crm_debug_4("Testing %s->%s for %s: %s vs. %s %s",
		    order->lh_action_task, order->rh_action_task, child_rsc->id,
		    role2text(lh_role), role2text(child_role), order->lh_action_task);
	
	if(rh_hosts == NULL) {
	    crm_debug_3("Terminating search: %s.%d list is empty: no possible %s",
			order->rh_rsc->id, down_stack, reason);
	    break;
	}

	if(lh_role_new == lh_role_old) {
	    restart = detect_restart(child_rsc);
	    if(restart == FALSE) {
		crm_debug_3("Ignoring %s->%s for %s: no relevant %s (no role change)",
			 order->lh_action_task, order->rh_action_task, child_rsc->id, reason);
		continue;
	    }
	}

	hosts = NULL;
	child_rsc->fns->location(child_rsc, &hosts, down_stack);
	intersection = node_list_and(hosts, rh_hosts, FALSE);
	/* slist_iter(h, node_t, hosts, llpc, crm_info("H: %s %s", child_rsc->id, h->details->uname)); */
	if(intersection == NULL) {
	    crm_debug_3("Ignoring %s->%s for %s: no relevant %s",
		     order->lh_action_task, order->rh_action_task, child_rsc->id, reason);
	    g_list_free(hosts);
	    continue;  
	}
			
	if(restart) {
	    reason = "restart";
	    create = TRUE;
			    
	} else if(down_stack) {
	    if(lh_role_old > lh_role) {
		create = TRUE;
	    }
			    
	} else if(down_stack == FALSE) {
	    if(lh_role_old < lh_role) {
		create = TRUE;
	    }

	} else {
	    any_ordered++;
	    reason = "role";
	    crm_debug_4("Role: %s->%s for %s: %s vs. %s %s",
		       order->lh_action_task, order->rh_action_task, child_rsc->id,
		       role2text(lh_role_old), role2text(lh_role), order->lh_action_task);
			    
	}

	if(create) {
	    char *task = order->lh_action_task;

	    any_ordered++;
	    crm_debug("Enforcing %s->%s for %s on %s: found %s",
		      order->lh_action_task, order->rh_action_task, child_rsc->id,
		      ((node_t*)intersection->data)->details->uname, reason);

	    order->lh_action_task = convert_non_atomic_task(task, child_rsc, TRUE, FALSE);
	    child_rsc->cmds->rsc_order_lh(child_rsc, order, data_set);
	    crm_free(order->lh_action_task);
	    order->lh_action_task = task;
	}
			
	crm_debug_3("Processed %s->%s for %s on %s: %s",
		    order->lh_action_task, order->rh_action_task, child_rsc->id,
		    ((node_t*)intersection->data)->details->uname, reason);
	
	/* slist_iter(h, node_t, hosts, llpc, */
	/* 	   crm_info("H: %s %s", child_rsc->id, h->details->uname)); */
			
	slist_destroy(node_t, node, intersection, crm_free(node));
	g_list_free(hosts);
			
	);
		    
    g_list_free(rh_hosts);

    if(any_ordered == 0 && down_stack == FALSE) {
	GListPtr lh_hosts = NULL;
	if(order->type & pe_order_runnable_left) {
	    rsc->fns->location(rsc, &lh_hosts, FALSE);
	}
	if(lh_hosts == NULL) {
	    order->lh_action_task = convert_non_atomic_task(order->lh_action_task, rsc, TRUE, TRUE);
	    native_rsc_order_lh(rsc, order, data_set);			
	}
	g_list_free(lh_hosts);
    }
    order->type = pe_order_optional;
}

void clone_rsc_order_lh(resource_t *rsc, order_constraint_t *order, pe_working_set_t *data_set)
{
	resource_t *r1 = NULL;
	resource_t *r2 = NULL;	
	gboolean do_interleave = FALSE;
	clone_variant_data_t *clone_data = NULL;
	get_clone_variant_data(clone_data, rsc);

	crm_debug_4("%s->%s", order->lh_action_task, order->rh_action_task);
	if(order->rh_rsc == NULL) {
	    order->lh_action_task = convert_non_atomic_task(order->lh_action_task, rsc, FALSE, TRUE);
	    native_rsc_order_lh(rsc, order, data_set);
	    return;
	}
	
	r1 = uber_parent(rsc);
	r2 = uber_parent(order->rh_rsc);
	
	if(r1 == r2) {
		native_rsc_order_lh(rsc, order, data_set);
		return;
	}
	
	if(order->rh_rsc->variant > pe_group && clone_data->interleave) {
	    clone_variant_data_t *clone_data_rh = NULL;
	    get_clone_variant_data(clone_data_rh, order->rh_rsc);


	    if(clone_data->clone_node_max == clone_data_rh->clone_node_max) {
		/* only the LHS side needs to be labeled as interleave */
		do_interleave = TRUE;

	    } else {
		crm_config_err("Cannot interleave "XML_CIB_TAG_INCARNATION
			       " %s and %s because they do not support the same"
			       " number of resources per node",
			       rsc->id, order->rh_rsc->id);
	    }
	}
	

	if(order->rh_rsc == NULL) {
	    do_interleave = FALSE;
	}
	
	if(do_interleave) {
	    resource_t *lh_child = NULL;
	    resource_t *rh_saved = order->rh_rsc;
	    gboolean current = FALSE;
	    
	    if(strstr(order->lh_action_task, "_stop_0") || strstr(order->lh_action_task, "_demote_0")) {
		current = TRUE;
	    }

	    slist_iter(
		rh_child, resource_t, rh_saved->children, lpc,
		
		CRM_ASSERT(rh_child != NULL);
		lh_child = find_compatible_child(rh_child, rsc, RSC_ROLE_UNKNOWN, current);
		if(lh_child == NULL && current) {
		    continue;
		    
		} else if(lh_child == NULL) {
		    crm_debug("No match found for %s (%d)", rh_child->id, current);

		    /* Me no like this hack - but what else can we do?
		     *
		     * If there is no-one active or about to be active
		     *   on the same node as rh_child, then they must
		     *   not be allowed to start
		     */
		    if(order->type & (pe_order_runnable_left|pe_order_implies_right) /* Mandatory */) {
			crm_info("Inhibiting %s from being active", rh_child->id);
			assign_node(rh_child, NULL, TRUE);
		    }
		    continue;
		}
		crm_debug("Pairing %s with %s", lh_child->id, rh_child->id);
		order->rh_rsc = rh_child;
		lh_child->cmds->rsc_order_lh(lh_child, order, data_set);
		order->rh_rsc = rh_saved;
		);
	    
	} else {
	    
#if 0
	    if(order->type != pe_order_optional) {
		crm_debug("Upgraded ordering constraint %d - 0x%.6x", order->id, order->type);
		native_rsc_order_lh(rsc, order, data_set);
	    }
#endif

	    if(order->rh_rsc->variant < pe_clone) {
		clone_rsc_order_lh_non_clone(rsc, order, data_set);		
		    
	    } else if(order->type & pe_order_implies_left) {
		if(rsc->variant == order->rh_rsc->variant) {
		    crm_debug_2("Clone-to-clone ordering: %s -> %s 0x%.6x",
				order->lh_action_task, order->rh_action_task, order->type);
		    /* stop instances on the same nodes as stopping RHS instances */
		    slist_iter(
			child_rsc, resource_t, rsc->children, lpc,
			native_rsc_order_lh(child_rsc, order, data_set);
			);
		} else {
		    /* stop everything */
		    slist_iter(
			child_rsc, resource_t, rsc->children, lpc,
			native_rsc_order_lh(child_rsc, order, data_set);
			);
		}
	    }
	}	
	
	if(do_interleave == FALSE || clone_data->ordered) {
	    order->lh_action_task = convert_non_atomic_task(order->lh_action_task, rsc, FALSE, TRUE);
	    native_rsc_order_lh(rsc, order, data_set);
	}	    
	
	if(is_set(rsc->flags, pe_rsc_notify)) {
	    order->type = pe_order_optional;
	    order->lh_action_task = convert_non_atomic_task(order->lh_action_task, rsc, TRUE, TRUE);
	    native_rsc_order_lh(rsc, order, data_set);
	}
}

static void clone_rsc_order_rh_non_clone(
    resource_t *lh_p, action_t *lh_action, resource_t *rsc, order_constraint_t *order)
{
    GListPtr lh_hosts = NULL;
    GListPtr intersection = NULL;
    const char *reason = "unknown";

    gboolean restart = FALSE;
    gboolean down_stack = TRUE;

    enum rsc_role_e rh_role = RSC_ROLE_STARTED;
    enum action_tasks task = start_rsc;

    enum rsc_role_e lh_role_new = lh_p->fns->state(lh_p, FALSE);
    enum rsc_role_e lh_role_old = lh_p->fns->state(lh_p, TRUE);

    /* Make sure the pre-req will be active */
    if(order->type & pe_order_runnable_left) {
	lh_p->fns->location(lh_p, &lh_hosts, FALSE);
	if(lh_hosts == NULL) {
	    crm_debug("Terminating search: Pre-requisite %s of %s is unrunnable", lh_p->id, rsc->id);
	    native_rsc_order_rh(lh_action, rsc, order);
	    return;
	}
	g_list_free(lh_hosts); lh_hosts = NULL;
    }

    if(strstr(order->lh_action_task, "_"RSC_STOP"_0")
       || strstr(order->lh_action_task, "_"RSC_STOPPED"_0")) {
	task = stop_rsc;
	reason = "down activity";
	rh_role = RSC_ROLE_STOPPED;
	lh_p->fns->location(lh_p, &lh_hosts, down_stack);

/* These actions are not possible for non-clones
   } else if(strstr(order->lh_action_task, "_"RSC_DEMOTE"_0")
   || strstr(order->lh_action_task, "_"RSC_DEMOTED"_0")) {
   task = action_demote;
   rh_role = RSC_ROLE_SLAVE;
   reason = "demotion activity";
   lh_p->fns->location(lh_p, &lh_hosts, down_stack);
		
   } else if(strstr(order->lh_action_task, "_"RSC_PROMOTE"_0")
   || strstr(order->lh_action_task, "_"RSC_PROMOTED"_0")) {
   task = action_promote;
   down_stack = FALSE;
   reason = "promote activity";
   lh_p->fns->location(lh_p, &lh_hosts, down_stack);
   rh_role = RSC_ROLE_MASTER;
*/
    } else if(strstr(order->lh_action_task, "_"RSC_START"_0")
	      || strstr(order->lh_action_task, "_"RSC_STARTED"_0")) {
	task = start_rsc;
	down_stack = FALSE;
	reason = "up activity";
	lh_p->fns->location(lh_p, &lh_hosts, down_stack);

    } else {
	crm_err("Unknown action: %s", order->lh_action_task);
	return;
    }
	    
    if(lh_role_new == lh_role_old) {
	restart = detect_restart(lh_action->rsc);
		
	if(FALSE && restart == FALSE) {
	    crm_debug_3("Ignoring %s->%s for %s: no relevant %s (no role change)",
			lh_action->task, order->lh_action_task, lh_p->id, reason);
	    goto cleanup;
	}
    }

    /* slist_iter(h, node_t, lh_hosts, llpc, crm_info("LHH: %s", h->details->uname)); */
    slist_iter(
	child_rsc, resource_t, rsc->children, lpc,

	gboolean create = FALSE;
	enum rsc_role_e child_role = child_rsc->fns->state(child_rsc, down_stack);

	crm_debug_4("Testing %s->%s for %s: %s vs. %s %s",
		    lh_action->task, order->lh_action_task, child_rsc->id,
		    role2text(rh_role), role2text(child_role), order->lh_action_task);
	
	if(lh_hosts == NULL) {
	    crm_debug_3("Terminating search: %s.%d list is empty: no possible %s",
			order->rh_rsc->id, down_stack, reason);
	    break;
	}

	intersection = NULL;
	if(task == stop_rsc) {
	    /* Only relevant for stopping stacks */

	    GListPtr hosts = NULL;
	    child_rsc->fns->location(child_rsc, &hosts, down_stack);
	    intersection = node_list_and(hosts, lh_hosts, FALSE);
	    g_list_free(hosts);
	    if(intersection == NULL) {
		crm_debug_3("Ignoring %s->%s for %s: no relevant %s",
			    lh_action->task, order->lh_action_task, child_rsc->id, reason);
		continue;
	    }
	}
	
	/* slist_iter(h, node_t, hosts, llpc, crm_info("H: %s %s", child_rsc->id, h->details->uname)); */
	if(restart) {
	    reason = "restart";
	    create = TRUE;
			    
	} else if(down_stack && lh_role_old >= rh_role) {
	    create = TRUE;
		    
	} else if(down_stack == FALSE && lh_role_old <= rh_role) {
	    create = TRUE;
		    
	} else {
	    reason = "role";
	}

	if(create) {
	    enum pe_ordering type = order->type;
	    child_rsc->cmds->rsc_order_rh(lh_action, child_rsc, order);
	    order->type = pe_order_optional;
	    native_rsc_order_rh(lh_action, rsc, order);
	    order->type = type;
	}
	
	crm_debug_3("Processed %s->%s for %s: found %s%s",
		    lh_action->task, order->lh_action_task, child_rsc->id, reason, create?" - enforced":"");
	
	/* slist_iter(h, node_t, hosts, llpc, */
	/* 	   crm_info("H: %s %s", child_rsc->id, h->details->uname)); */
		
	slist_destroy(node_t, node, intersection, crm_free(node));
	);
  cleanup:	    
    g_list_free(lh_hosts);
}

void clone_rsc_order_rh(
	action_t *lh_action, resource_t *rsc, order_constraint_t *order)
{
	enum pe_ordering type = order->type;
	clone_variant_data_t *clone_data = NULL;
	resource_t *lh_p = uber_parent(lh_action->rsc);
	
	get_clone_variant_data(clone_data, rsc);
	crm_debug_2("%s->%s", order->lh_action_task, order->rh_action_task);

	if(safe_str_eq(CRM_OP_PROBED, lh_action->uuid)) {
	    slist_iter(
		child_rsc, resource_t, rsc->children, lpc,
		child_rsc->cmds->rsc_order_rh(lh_action, child_rsc, order);
		);

	    if(rsc->fns->state(rsc, TRUE) < RSC_ROLE_STARTED
		&& rsc->fns->state(rsc, FALSE) > RSC_ROLE_STOPPED) {
		order->type |= pe_order_implies_right;
	    }

	} else if(lh_p && lh_p != rsc && lh_p->variant < pe_clone) {
	    clone_rsc_order_rh_non_clone(lh_p, lh_action, rsc, order);
	    return;
	}
	
 	native_rsc_order_rh(lh_action, rsc, order);
	order->type = type;
}

void clone_rsc_location(resource_t *rsc, rsc_to_node_t *constraint)
{
	clone_variant_data_t *clone_data = NULL;
	get_clone_variant_data(clone_data, rsc);

	crm_debug_3("Processing location constraint %s for %s",
		    constraint->id, rsc->id);

	native_rsc_location(rsc, constraint);
	slist_iter(
		child_rsc, resource_t, rsc->children, lpc,

		child_rsc->cmds->rsc_location(child_rsc, constraint);
		);
}


void clone_expand(resource_t *rsc, pe_working_set_t *data_set)
{
	clone_variant_data_t *clone_data = NULL;
	get_clone_variant_data(clone_data, rsc);

	crm_debug_2("Processing actions from %s", rsc->id);
	
	if(clone_data->start_notify) {
	    collect_notification_data(rsc, TRUE, TRUE, clone_data->start_notify);
	    expand_notification_data(clone_data->start_notify);
	    create_notifications(rsc, clone_data->start_notify, data_set);
	}

	if(clone_data->stop_notify) {
	    collect_notification_data(rsc, TRUE, TRUE, clone_data->stop_notify);
	    expand_notification_data(clone_data->stop_notify);
	    create_notifications(rsc, clone_data->stop_notify, data_set);
	}
	
	if(clone_data->promote_notify) {
	    collect_notification_data(rsc, TRUE, TRUE, clone_data->promote_notify);
	    expand_notification_data(clone_data->promote_notify);
	    create_notifications(rsc, clone_data->promote_notify, data_set);
	}
	
	if(clone_data->demote_notify) {
	    collect_notification_data(rsc, TRUE, TRUE, clone_data->demote_notify);
	    expand_notification_data(clone_data->demote_notify);
	    create_notifications(rsc, clone_data->demote_notify, data_set);
	}
	
	/* Now that the notifcations have been created we can expand the children */
	slist_iter(
		child_rsc, resource_t, rsc->children, lpc,		
		child_rsc->cmds->expand(child_rsc, data_set));

	native_expand(rsc, data_set);

	/* The notifications are in the graph now, we can destroy the notify_data */
	free_notification_data(clone_data->demote_notify);  clone_data->demote_notify = NULL;
	free_notification_data(clone_data->stop_notify);    clone_data->stop_notify = NULL;
	free_notification_data(clone_data->start_notify);   clone_data->start_notify = NULL;
	free_notification_data(clone_data->promote_notify); clone_data->promote_notify = NULL;
}


static gint sort_rsc_id(gconstpointer a, gconstpointer b)
{
	const resource_t *resource1 = (const resource_t*)a;
	const resource_t *resource2 = (const resource_t*)b;

	CRM_ASSERT(resource1 != NULL);
	CRM_ASSERT(resource2 != NULL);

	return strcmp(resource1->id, resource2->id);
}

static resource_t *find_instance_on(resource_t *rsc, node_t *node)
{
    slist_iter(child, resource_t, rsc->children, lpc,
	       GListPtr known_list = NULL;
	       rsc_known_on(child, &known_list); 
	       slist_iter(known, node_t, known_list, lpc2,
			  if(node->details == known->details) {
			      g_list_free(known_list);
			      return child;
			  }
		   );
	       g_list_free(known_list);	       
	);
    return NULL;
}

gboolean
clone_create_probe(resource_t *rsc, node_t *node, action_t *complete,
		    gboolean force, pe_working_set_t *data_set) 
{
	gboolean any_created = FALSE;
	clone_variant_data_t *clone_data = NULL;
	get_clone_variant_data(clone_data, rsc);

	rsc->children = g_list_sort(rsc->children, sort_rsc_id);
	if(rsc->children == NULL) {
	    pe_warn("Clone %s has no children", rsc->id);
	    return FALSE;
	}
	
	if(is_not_set(rsc->flags, pe_rsc_unique)
	   && clone_data->clone_node_max == 1) {
		/* only look for one copy */	 
		resource_t *child = NULL;

		/* Try whoever we probed last time */
		child = find_instance_on(rsc, node);
		if(child) {
		    return child->cmds->create_probe(
			child, node, complete, force, data_set);
		}

		/* Try whoever we plan on starting there */
		slist_iter(	 
			child_rsc, resource_t, rsc->children, lpc,	 

			node_t *local_node = child_rsc->fns->location(child_rsc, NULL, FALSE);
			if(local_node == NULL) {
			    continue;
			}
			
			if(local_node->details == node->details) {
			    return child_rsc->cmds->create_probe(
				child_rsc, node, complete, force, data_set);
			}
		    );

		/* Fall back to the first clone instance */
		child = rsc->children->data;
		return child->cmds->create_probe(child, node, complete, force, data_set);
	}
	slist_iter(
		child_rsc, resource_t, rsc->children, lpc,

		if(child_rsc->cmds->create_probe(
			   child_rsc, node, complete, force, data_set)) {
			any_created = TRUE;
		}
		
		if(any_created
		   && is_not_set(rsc->flags, pe_rsc_unique)
		   && clone_data->clone_node_max == 1) {
			/* only look for one copy (clone :0) */	 
			break;
		}
		);

	return any_created;
}

void clone_append_meta(resource_t *rsc, xmlNode *xml)
{
    char *name = NULL;
    clone_variant_data_t *clone_data = NULL;
    get_clone_variant_data(clone_data, rsc);

    name = crm_meta_name(XML_RSC_ATTR_UNIQUE);
    crm_xml_add(xml, name, is_set(rsc->flags, pe_rsc_unique)?"true":"false");
    crm_free(name);

    name = crm_meta_name(XML_RSC_ATTR_NOTIFY);
    crm_xml_add(xml, name, is_set(rsc->flags, pe_rsc_notify)?"true":"false");
    crm_free(name);
    
    name = crm_meta_name(XML_RSC_ATTR_INCARNATION_MAX);
    crm_xml_add_int(xml, name, clone_data->clone_max);
    crm_free(name);

    name = crm_meta_name(XML_RSC_ATTR_INCARNATION_NODEMAX);
    crm_xml_add_int(xml, name, clone_data->clone_node_max);
    crm_free(name);
}
