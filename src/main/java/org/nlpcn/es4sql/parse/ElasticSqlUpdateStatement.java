package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

import java.util.ArrayList;
import java.util.List;

//todo::修改内容
public class ElasticSqlUpdateStatement extends MySqlUpdateStatement {

    private List<SQLCommentHint> hints;

    public List<SQLCommentHint> getHints() {
        if (hints == null) {
            hints = new ArrayList<SQLCommentHint>(2);
        }

        return hints;
    }

}
