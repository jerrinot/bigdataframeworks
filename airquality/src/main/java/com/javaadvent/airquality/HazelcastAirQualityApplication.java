package com.javaadvent.airquality;

import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.TestSources;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;

public class HazelcastAirQualityApplication {

	private static final int HIGH_THRESHOLD = 10;

	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Usage: application <comma_separated_values>");
			return;
		}

		String[] numbers = args[0].split(",");
		System.out.println("Measuring for air quality values: " + args[0]);
		new HazelcastAirQualityApplication().countPollutedRegions(numbers);
	}

	public long countPollutedRegions(String[] numbers) {

		Pipeline p = Pipeline.create();
		p.readFrom(TestSources.itemStream(100, (ts, seq) -> HashUtil.fastLongMix(ts) % 20))
				.withIngestionTimestamps()
				.filter(number -> number > HIGH_THRESHOLD)
				.window(WindowDefinition.tumbling(1000))
				.aggregate(counting())
				.map(WindowResult::result)
				.writeTo(Sinks.observable("filteredNumbers"));

		JetInstance jet = Jet.newJetInstance();
		try {
			jet.newJob(p);
			Iterable<Long> observableIterator = ObservableIterable.byName(jet, "filteredNumbers");
			for (long l : observableIterator) {
				System.out.println(l);
			}

			return -1;
		} finally {
			Jet.shutdownAll();
		}
	}
}
