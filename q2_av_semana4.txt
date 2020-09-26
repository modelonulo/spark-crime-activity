

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

        JavaRDD<String> filtrado = arquivo.filter(s -> {
            String[] campos = s.split(";");
            String tipo = campos[4];
            if(tipo.equalsIgnoreCase("NARCOTICS")){
                return true;
            }
        return false;
        
        });
        
        
        JavaRDD<String> anoRDD = filtrado.map(s -> {
            String[] campos = s.split(;);
            return campos[2];
        });

        System.out.println(anoRDD.countByValue()); 

    }

} 

{2014=547438, 2003=944292, 2013=612698, 2002=944160, 2018=238950, 2007=871402, 2004=934472, 2015=515922, 2011=702608, 2001=965880, 
2008=840362, 2012=670854, 2005=900024, 2010=739988, 2009=772350, 2016=534500, 2017=530138, 2006=891146} 
