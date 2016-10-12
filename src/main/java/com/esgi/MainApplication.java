package com.esgi;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.esgi.Utils.Parser;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.*;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
/**
 * Created by Karthi on 09/10/2016.
 */
public class MainApplication {


    public static void main (String[] args){

        Cluster cluster;
        Session session;
        ResultSet results;
        Row rows;

        cluster = Cluster
                .builder()
                .addContactPoint("127.0.0.1")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .build();

        session = cluster.connect("nosql");
        session.execute("drop table if exists nosql.article");
        session.execute("drop table if exists nosql.author");
        session.execute("drop table if exists nosql.articleAuthor");

        //Parser p1 = new Parser()
        //p1.InsertXmlToCassandra(System.getProperty("user.dir") + "/test_file_xml.xml");
        session.execute("create table article (id int primary key, mdate varchar, key varchar, title varchar, pages varchar, year int, volume int, journal varchar, number int, ee varchar, url varchar)");
        session.execute("create table author (id int primary key, name varchar, firstname varchar)");
        session.execute("create table articleAuthor (id int primary key, idArticle int , idAuthor int)");

        try {
            File fXmlFile = new File(System.getProperty("user.dir") + "/test_file_xml.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);

            doc.getDocumentElement().normalize();

            NodeList nList = doc.getElementsByTagName("article");
            String mdate, key, title, journal, ee, url, pages, name, firstname;
            int number = 0;
            int volume = 0;
            int year = 0;

            String author = " ";
            int id = 0;
            int idAuthor = 0;
            int idArticleAuthor = 0;
            for (int temp = 0; temp < nList.getLength(); temp++) {

                Node nNode = nList.item(temp);

                Element eElement = (Element) nNode;
                mdate = eElement.getAttribute("mdate");
                key = eElement.getAttribute("key");

                NodeList authorNode = ((Element) nNode).getElementsByTagName("author");
                for (int i = 0; i < authorNode.getLength(); i++) {
                    if (authorNode.item(i) != null){
                        int espace = authorNode.item(i).getTextContent().lastIndexOf(" ");
                        firstname = authorNode.item(i).getTextContent().substring(0, espace -1);
                        name = authorNode.item(i).getTextContent().substring(espace+1);

                        name = name.replaceAll("'"," ");
                        firstname = firstname.replaceAll("'"," ");

                        ResultSet result = session.execute("select * from nosql.author where " +
                                "name = '"+name+"' " +
                                "and firstname = '"+firstname+"'" +
                                "allow filtering");

                        if (result.one() == null) {
                            session.execute("insert into nosql.author (id,firstname,name)" +
                                    " values (" + idAuthor + ",'" + firstname + "','" + name + "')");

                            session.execute("insert into nosql.articleAuthor (id, idArticle,idAuthor)" +
                                    " values ("+idArticleAuthor+","+ id + "," + idAuthor + ")");

                            idAuthor++;
                            idArticleAuthor++;
                        }
                    }

                }

                if (eElement.getElementsByTagName("title").item(0) != null){


                    title = StringEscapeUtils.escapeHtml(eElement.getElementsByTagName("title").item(0).getTextContent());
                } else {
                    title = null;
                }

                if (eElement.getElementsByTagName("pages").item(0) != null){
                    pages = eElement.getElementsByTagName("pages").item(0).getTextContent();
                } else {
                    pages = null;
                }

                if (eElement.getElementsByTagName("year").item(0) != null){
                    year = Integer.parseInt(eElement.getElementsByTagName("year").item(0).getTextContent());
                } else {
                    year = 0;
                }

                if (eElement.getElementsByTagName("volume").item(0) != null){
                    volume = Integer.parseInt(eElement.getElementsByTagName("volume").item(0).getTextContent());
                } else {
                    volume = 0;
                }

                if (eElement.getElementsByTagName("journal").item(0) != null){
                    journal = eElement.getElementsByTagName("journal").item(0).getTextContent();
                } else {
                    journal = null;
                }

                if (eElement.getElementsByTagName("number").item(0) != null){
                    number = Integer.parseInt(eElement.getElementsByTagName("number").item(0).getTextContent());
                } else {
                    number = 0;
                }

                if (eElement.getElementsByTagName("ee").item(0) != null){
                    ee = eElement.getElementsByTagName("ee").item(0).getTextContent();
                } else {
                    ee = null;
                }

                if (eElement.getElementsByTagName("url").item(0) != null){
                    url = eElement.getElementsByTagName("url").item(0).getTextContent();
                } else {
                    url = null;
                }

                String titleEscaped = title.replaceAll("'"," ");
                session.execute("insert into nosql.article (id,mdate,key, title, pages, year, volume, journal, number, ee , url)" +
                       " values ("+id+",'"+mdate+"','"+key+"','"+ titleEscaped +"', '"+pages+"' ,"+ year+","+ volume +",'"+ journal +"',"+ number +",'"+ ee +"','"+url+"' )");


                id++;

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        Statement select = QueryBuilder.select().all().from("nosql", "article");

        results = session.execute(select);
        for (Row row : results) {
            System.out.println(row);
        }

    }


}
