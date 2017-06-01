//package com.test;
//
//import junit.framework.TestCase;
//import net.sf.jsqlparser.expression.ExpressionVisitor;
//import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
//import net.sf.jsqlparser.parser.CCJSqlParserUtil;
//import net.sf.jsqlparser.schema.Table;
//import net.sf.jsqlparser.statement.Statement;
//import net.sf.jsqlparser.statement.select.FromItemVisitor;
//import net.sf.jsqlparser.statement.select.PlainSelect;
//import net.sf.jsqlparser.statement.select.Select;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
///**
// * Created by houlongbin on 2016/11/28.
// */
//public class TablesNamesFinder extends TestCase{
//    public void testLeftSemiJoin() throws Exception {
//        String sql;
//        Statement statement;
//
//        sql = "SELECT\n"
//                + "    Something\n"
//                + "FROM\n"
//                + "    Sometable\n"
//                + "LEFT SEMI JOIN\n"
//                + "    Othertable\n";
//
//        statement = CCJSqlParserUtil.parse(sql);
//
//        System.out.println(statement.toString());
//
//        Select select = (Select) statement;
//        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
//        assertEquals(1, plainSelect.getJoins().size());
//        assertEquals("Othertable", ((Table) plainSelect.getJoins().get(0).getRightItem()).getFullyQualifiedName());
//        assertTrue(plainSelect.getJoins().get(0).isLeft());
//        assertTrue(plainSelect.getJoins().get(0).isSemi());
//    }
//}
