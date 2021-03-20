

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

        JavaRDD<String> arquivo = sc.textFile("/home/camila/Nextcloud/ProjetosJava/Spark/ocorrencias_criminais.csv");

        JavaRDD<String> mesRDD = arquivo.map(s -> {
            String[] campos = s.split(";");
            String mes = campos[1];
            String ano = campos[2];
            return String.join("/", ano, mes);
        });

        Map<String, Long> meses = mesRDD.countByValue();

        Stream<Map.Entry<String, Long>> stream =
                meses.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()));

        Map<String, Long> resultado = stream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        for (Map.Entry<String, Long> entrada : resultado.entrySet()) {
            String chave = entrada.getKey();
            Long valor = entrada.getValue();
            System.out.println(String.format("key: %s | value: %d", chave, valor / 12));
        }

        System.out.println("==================================");


    }
}

key: 2002/07 | value: 7498
key: 2001/07 | value: 7395
key: 2001/08 | value: 7292
key: 2003/08 | value: 7265
key: 2002/08 | value: 7212
key: 2004/07 | value: 7184
key: 2003/07 | value: 7178
key: 2004/08 | value: 7153
key: 2003/10 | value: 7141
key: 2001/10 | value: 7133
key: 2002/05 | value: 7010
key: 2002/06 | value: 6983
key: 2002/10 | value: 6972
key: 2001/05 | value: 6931
key: 2005/07 | value: 6928
key: 2001/06 | value: 6911
key: 2002/09 | value: 6901
key: 2004/10 | value: 6900
