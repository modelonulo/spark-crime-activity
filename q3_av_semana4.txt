

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

        JavaRDD<String> filtrado;
        filtrado = arquivo.filter(s ->{
            String[] campos = s.split(";");

            String tipo = campos[4];

            int dia = Integer.parseInt(campos[0]);

            if(tipo.equalsIgnoreCase("NARCOTICS") && (dia % 2 ==0)) {
                return true;
            } else {
                return false;
            }
        });
        
        JavaRDD<String> anoRDD = filtrado.map(s -> {
            String[] campos = s.split(;);
            return campos[2];
        });

         System.out.println(anoRDD.countByValue()); 

    }

} 

2014=57812, 2003=107542, 2013=68212, 2002=99704, 2018=11576, 2007=108824, 2004=113764, 2015=43216, 
2011=77158, 2001=100644, 2008=90144, 2012=70942, 2005=111904, 2010=86780, 2009=84328, 2016=26514, 2017=22950, 2006=110684} 
