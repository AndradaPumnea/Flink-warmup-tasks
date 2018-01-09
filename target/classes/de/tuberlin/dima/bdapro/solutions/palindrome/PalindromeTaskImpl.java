package de.tuberlin.dima.bdapro.solutions.palindrome;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;

public class PalindromeTaskImpl implements PalindromeTask {

	@Override
	public Set<String> solve(String inputFile) throws Exception {

		//******************************
		//*Implement your solution here*
		//******************************

		Set<String> finalResult = new HashSet<String>();

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


		DataSet<String> text = env.readTextFile(inputFile);

		DataSet<Tuple2<String, Integer>> palin = text.flatMap(new CheckPalindrome());

		DataSet<Tuple2<String, Integer>> maxPalin = palin.aggregate(Aggregations.MAX, 1);

		JoinOperator.EquiJoin<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> allMaxPalin = palin.join(maxPalin)
				.where(1)
				.equalTo(1)
				.with(new JoinMaxTuples());

		DataSet<String> prepareResult = allMaxPalin.flatMap(new PrepareOutput());

		List<String> result = prepareResult.collect();
		for (String str : result){
			finalResult.add(str);
		}

		return finalResult;
	}

	public static final class CheckPalindrome implements FlatMapFunction<String, Tuple2<String,Integer>> {

		@Override
		public void flatMap(String sentence, Collector<Tuple2<String,Integer>> out) {

			String pattern = "[^a-zA-Z0-9]";
			String newString = sentence.replaceAll(pattern, "");

			int i = 0;
			int j = newString.length() - 1;
			boolean palindrome = true;

			while (i < j){
				if(newString.charAt(i) != newString.charAt(j)) {
					palindrome = false;
					break;
				} else {
					i++;
					j--;
				}
			}

			if (palindrome == true){
				out.collect(new Tuple2<String, Integer>(sentence, newString.length()));
			}
		}
	}


	public class JoinMaxTuples implements JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> join(Tuple2<String, Integer> palin, Tuple2<String, Integer> maxPalin) {
			// multiply the points and rating and construct a new output tuple
			return new Tuple2<String, Integer>(palin.f0, maxPalin.f1);
		}
	}

	public static final class PrepareOutput implements FlatMapFunction<Tuple2<String,Integer>, String> {

		@Override
		public void flatMap(Tuple2<String,Integer> palin, Collector<String> out) {
			out.collect(palin.f0);
		}
	}


}
