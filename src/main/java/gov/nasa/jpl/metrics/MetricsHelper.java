package gov.nasa.jpl.jsearch.metrics;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.*;

import java.util.*;

public class MetricsHelper {
    private String appName;
    private List<Dimension> dimensions;

    private Map<String, MetricDatum> metrics;

    public MetricsHelper(String namespace, String appName) {
        this.appName = appName;
        this.dimensions = new ArrayList<Dimension>();
        this.dimensions.add(new Dimension().withName("namespace").withValue(namespace));
        this.metrics = new HashMap<String, MetricDatum>();
    }

    public MetricsHelper(String namespace, String appName, String component) {
        this(namespace, appName);
        this.dimensions.add(new Dimension().withName("component").withValue(component));
    }

    public MetricsHelper recordTime(String name, long startTime) {
        double timing = System.currentTimeMillis() - startTime;
        return record(name, timing, StandardUnit.Milliseconds);
    }

    public MetricsHelper recordScalar(String name, int scalar) {
        return record(name, scalar, StandardUnit.Count);
    }

    public MetricsHelper recordScalar(String name) {
        return recordScalar(name, 1);
    }

    private MetricsHelper record(String name, Number number, StandardUnit unit) {
        StatisticSet statisticSet;

        if (!metrics.containsKey(name)) {
            statisticSet = new StatisticSet();
            statisticSet.setSampleCount(0.0);
            statisticSet.setSum(0.0);
            statisticSet.setMinimum(number.doubleValue());
            statisticSet.setMaximum(number.doubleValue());

            MetricDatum datum = new MetricDatum()
                    .withMetricName(name)
                    .withStatisticValues(statisticSet)
                    .withUnit(unit)
                    .withTimestamp(new Date())
                    .withStorageResolution(60)
                    .withDimensions(this.dimensions);

            metrics.put(name, datum);
        } else {
            MetricDatum datum = metrics.get(name);
            statisticSet = datum.getStatisticValues();
        }

        statisticSet.setSampleCount(statisticSet.getSampleCount() + 1);
        statisticSet.setSum(statisticSet.getSum() + number.doubleValue());
        if (number.doubleValue() < statisticSet.getMinimum()) {
            statisticSet.setMinimum(number.doubleValue());
        }
        if (number.doubleValue() > statisticSet.getMaximum()) {
            statisticSet.setMaximum(number.doubleValue());
        }

        return this;
    }

    public void flush() {
        if (!metrics.isEmpty()) {
            final AmazonCloudWatch cw =
                    AmazonCloudWatchClientBuilder.defaultClient();
            PutMetricDataRequest request = new PutMetricDataRequest()
                    .withNamespace(appName)
                    .withMetricData(metrics.values());
            cw.putMetricData(request);
            this.metrics = new HashMap<String, MetricDatum>();
        }
    }
}
