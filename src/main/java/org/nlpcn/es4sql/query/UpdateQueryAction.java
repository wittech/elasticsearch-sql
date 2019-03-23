package org.nlpcn.es4sql.query;


import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.nlpcn.es4sql.domain.Delete;
import org.nlpcn.es4sql.domain.Update;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.maker.QueryMaker;

import java.util.Collections;
import java.util.List;

//todo::修改内容
public class UpdateQueryAction extends QueryAction {

	private final Update update;
	private UpdateByQueryRequestBuilder request;

	public UpdateQueryAction(Client client, Update update) {
		super(client, update);
		this.update = update;
	}

	@Override
	public SqlElasticUpdateByQueryRequestBuilder explain() throws SqlParseException {
		this.request = new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);

		setIndicesAndTypes();
		//todo::在设置where之前，将set中的内容转换成Script进行设置
		setItems(update.getSets());
		setWhere(update.getWhere());

		// maximum number of processed documents
		if (update.getRowCount() > -1) {
			request.size(update.getRowCount());
		}

		// set conflicts param
		updateRequestWithConflicts();

		SqlElasticUpdateByQueryRequestBuilder updateByQueryRequestBuilder = new SqlElasticUpdateByQueryRequestBuilder(request);
		return updateByQueryRequestBuilder;
	}

	/**
	 * 将set中的语句转换成Script进行执行
	 * */
	private void setItems(List<SQLUpdateSetItem> items){
		StringBuilder builder = new StringBuilder();
		for (SQLUpdateSetItem setItem : items){
			String colExpr = setItem.getColumn().toString().trim();
			if(colExpr.equals("$script") && setItem.getValue() instanceof SQLCharExpr){
				SQLCharExpr expr = (SQLCharExpr)setItem.getValue();
				builder.append(expr.getText());
				if(!expr.getText().endsWith(";")) {
					builder.append(";");
				}
			}
			else if(setItem.getValue() instanceof SQLCharExpr)
			{
				SQLCharExpr expr = (SQLCharExpr)setItem.getValue();
				builder.append("ctx._source.");
				builder.append(colExpr);
				builder.append("='");
				builder.append(expr.getText());
				builder.append("';");
			}
			else
			{
				String valExpr = setItem.getValue().toString().trim();
				builder.append("ctx._source.");
				builder.append(colExpr);
				builder.append("=");
				builder.append(valExpr);
				builder.append(";");
			}
		}

		Script newScript = new Script(ScriptType.INLINE, "painless", builder.toString(), Collections.emptyMap());
		//todo::在执行之前，将set里面的内容拼接成Script进行执行
		request.script(newScript);
	}


	/**
	 * Set indices and types to the delete by query request.
	 */
	private void setIndicesAndTypes() {
		UpdateByQueryRequest innerRequest = request.request();
		innerRequest.indices(query.getIndexArr());
		String[] typeArr = query.getTypeArr();
		if (typeArr != null) {
			innerRequest.getSearchRequest().types(typeArr);
		}
//		String[] typeArr = query.getTypeArr();
//		if (typeArr != null) {
//            request.set(typeArr);
//		}
	}


	/**
	 * Create filters based on
	 * the Where clause.
	 *
	 * @param where the 'WHERE' part of the SQL query.
	 * @throws SqlParseException
	 */
	private void setWhere(Where where) throws SqlParseException {
		if (where != null) {
			QueryBuilder whereQuery = QueryMaker.explan(where);
			request.filter(whereQuery);
		} else {
			request.filter(QueryBuilders.matchAllQuery());
		}
	}


//	/**
//	 * Set indices and types to the delete by query request.
//	 */
//	private void setIndicesAndTypes() {
//
//        DeleteByQueryRequest innerRequest = request.request();
//        innerRequest.indices(query.getIndexArr());
//        String[] typeArr = query.getTypeArr();
//        if (typeArr!=null){
//            innerRequest.getSearchRequest().types(typeArr);
//        }
////		String[] typeArr = query.getTypeArr();
////		if (typeArr != null) {
////            request.set(typeArr);
////		}
//	}

	private void updateRequestWithConflicts() {
		for (Hint hint : update.getHints()) {
			if (hint.getType() == HintType.CONFLICTS && hint.getParams() != null && 0 < hint.getParams().length) {
				String conflicts = hint.getParams()[0].toString();
				switch (conflicts) {
					case "proceed": request.abortOnVersionConflict(false); return;
					case "abort": request.abortOnVersionConflict(true); return;
					default: throw new IllegalArgumentException("conflicts may only be \"proceed\" or \"abort\" but was [" + conflicts + "]");
				}
			}
		}
	}

}
