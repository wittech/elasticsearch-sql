package org.nlpcn.es4sql.domain;

import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;

import java.util.ArrayList;
import java.util.List;

/**
 todo::修改内容
 */
public class Update extends Query{
    public List<SQLUpdateSetItem> getSets() {
        return sets;
    }

    public void setSets(List<SQLUpdateSetItem> sets) {
        this.sets = sets;
    }

    private List<SQLUpdateSetItem> sets = new ArrayList<>();

}
