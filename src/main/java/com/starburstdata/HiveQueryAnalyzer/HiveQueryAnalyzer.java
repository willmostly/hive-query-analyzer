package com.starburstdata.HiveQueryAnalyzer;

import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseError;
import org.apache.hadoop.hive.ql.parse.ParseException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HiveQueryAnalyzer
{

    Set<FunctionSignature> functions = new HashSet<FunctionSignature>();
    Set<String> tables = new HashSet<String>();
    Integer nJoins = 0;
    Integer nWindows = 0;
    Integer queryLength = 0;

    void analyze(String hql)
    {
        queryLength = hql.split("[\\s,]+").length;
        ParseDriver pd = new ParseDriver();
        try {
            ASTNode node = pd.parse(StringUtils.stripEnd(hql, ";"));
            walkTree(node);
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
//TOK_TABREF:954, TOK_FROM:748, TOK_TABNAME:953, TOK_FUNCTION:750, CommonToken;26 (func name, child 0), TOK_JOIN 790, TOK_WINDOWSPEC:991
    void walkTree(ASTNode node)
    {
        if (node.getChildCount() == 0) {
            return;
        }
        //reference: https://github.com/apache/hive/blob/master/hplsql/src/main/antlr4/org/apache/hive/hplsql/Hplsql.g4

        for (Node child : node.getChildren())
        {
            switch ( ((ASTNode)child).getToken().getType()) {
                case 750: processFunction((ASTNode)child);
                break;
                case 953: processTable((ASTNode)child);
                break;
                case 790: processJoins();
                break;
                case 991: processWindow();
                break;
            }
            walkTree((ASTNode) child);
        }
    }

    void processJoins()
    {
        nJoins +=1;
    }

    void processFunction(ASTNode node)
    {
        functions.add(new FunctionSignature(node.getChild(0).getText(), node.getChildCount() -1));
    }

    void processTable(ASTNode node)
    {
        if (node.getChildCount() == 1) {
            tables.add(node.getChild(0).getText());
        } else if (node.getChildCount() == 2) {
            tables.add(node.getChild(0).getText() + "." + node.getChild(1).getText());
        }
    }

    void processWindow()
    {
        nWindows +=1;
    }

    void printReport(){
        System.out.println("Query length: " + queryLength.toString());
        System.out.println("Unique tables: " + Integer.toString(tables.size()));
        System.out.println("Number of Joins: " + nJoins.toString());
        System.out.println("Number of Window Functions: " + nWindows.toString());
        System.out.println("FUNCTIONS");
        for (FunctionSignature function : functions) {
            System.out.println("Name: " + function.name + " number of arguements: " + function.nArgs.toString());
        }
    }

    class FunctionSignature
    {
        String name;
        Integer nArgs;

        public String getName()
        {
            return name;
        }

        public Integer getnArgs()
        {
            return nArgs;
        }

        FunctionSignature(String name, int nArgs)
        {
            this.name = name;
            this.nArgs = nArgs;
        }

        @Override
        public boolean equals(Object object){
            if (object.getClass() != FunctionSignature.class) {
                return false;
            }

            if( name.equals(((FunctionSignature) object).getName()) && nArgs == ((FunctionSignature) object).getnArgs()) {
                return true;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return name.hashCode() + nArgs.hashCode();
        }
    }

    public static void main(String[] argv)
    {
        String hql = argv[0];
        boolean printHumanReadable = argv.length > 1 && argv[1].equals("printHumanReadable");

        HiveQueryAnalyzer analyzer = new HiveQueryAnalyzer();
        analyzer.analyze(hql);
        if (printHumanReadable) {
            analyzer.printReport();
        } else {
            Gson gson = new Gson();
            String json = gson.toJson(analyzer);
            System.out.println(json);
        }
    }
}
