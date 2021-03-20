

package com.mycompany.atividadespark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author camila.silveira
 */
public class Crime {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("pratica");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> arquivo = sc.textFile("hdfs://localhost/Frameworks/spark/ocorrencias_criminais.csv");

        
        JavaRDD<Long> mesRDD = arquivo.map(s -> { 
        
            String[] campos = s.split(";");
            return Long.parseLong(campos[1]);
            
        });
        
        Map<Long, Long> meses = mesRDD.countByValue();
        
        Stream<Map.Entry<Long, Long>> stream =
            meses.entrySet().stream()
            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()));
        
        Map<Long, Long> resultado = stream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        
        System.out.println(resultado.toString());
        System.out.println("==================================");
    }

} 
{7=1202032, 5=1197694, 8=1188690, 6=1177610, 10=1128926, 9=1116710, 4=1107858, 3=1105354, 1=1041962, 11=1015940, 12=956220, 2=918188} 
