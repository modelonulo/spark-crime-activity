package com.mycompany.atividadespark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author camila.silveira
 */
public class NossoMain {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("pratica");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> arquivo = sc.textFile("hdfs://localhost/Frameworks/spark/ocorrencias_criminais.csv");
w
        JavaRDD<String> anoRdd = arquivo.map(s -> {
            String[] campos = s.split(;);
            return campos[2];
        });

        System.out.println(anoRdd.countByValue()); 

    }

}

Resultado: {2014=914, 2003=100506, 2013=15, 2002=103798, 2018=118664, 2007=27, 2004=492, 2015=197887, 2011=53, 2001=737, 
2008=1621, 2012=84, 2005=54, 2010=21, 2009=21334, 2016=239189, 2017=214573, 2006=31} 
