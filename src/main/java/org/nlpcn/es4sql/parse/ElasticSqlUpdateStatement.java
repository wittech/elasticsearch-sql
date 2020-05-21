package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLHint;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

import java.util.ArrayList;
import java.util.List;

//todo::修改内容
public class ElasticSqlUpdateStatement extends MySqlUpdateStatement {

    private List<SQLHint> hints;

    public List<SQLHint> getHints() {
        if (hints == null) {
            hints = new ArrayList<SQLHint>(2);
        }

        return hints;
    }

}
