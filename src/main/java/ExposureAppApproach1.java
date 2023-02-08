import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;
import model.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.*;
import java.time.format.TextStyle;
import java.util.*;

@Slf4j
public class ExposureAppApproach1 {
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final int YEAR = 2022;
    public static double[] dailyChanges = new double[365];
    public static int nbrOfSkippedEvents = 0;
    public static List<String> failedEvents = new ArrayList<>();

    public static void main (String[] args) throws IOException {
        log.info("Application started!");

        BufferedReader reader = new BufferedReader(new FileReader("/Users/yuchen/IdeaProjects/Invoier/src/main/resources/events.json"));
        String line;

        while ((line = reader.readLine()) != null) {
            process(line);
        }

        double[] results = getMonthlyMaximumExposure(accumulator());
        printResults(results);
    }

    private static void process(String line) {
        EventType type = getEventType(line);
        if (isApplicable(type)) {
            try {
                handle(toEvent(line, type));
            } catch (Exception e) {
                // Noticed that there are a few entries where e.g. quote is missing, events missing fields compare
                // with other events of same type. Took the liberty to assume that this is a problem on the producer
                // side.
                failedEvents.add(line);
                // Log level could be set to error (if applicable)
                log.debug("Failed to process event!", line, e);
            }
        }
        // Would be good to log event id (or other unique identifier) here
        log.info("Skipping event!");
        nbrOfSkippedEvents++;
    }

    // This method could be replaced with a filter in case of Kafka consumer stream configured
    private static boolean isApplicable(EventType type) {
        return type == EventType.InvoiceRegistered || type == EventType.PaymentRegistered || type == EventType.LateFeeRegistered;
    }

    private static void printResults(double[] results) {
        System.out.println("------------- Final results -------------");

        for(int i = 0; i < results.length; i++) {
            System.out.println(String.format("%s: %s", Month.of(i + 1).getDisplayName(TextStyle.FULL, Locale.ENGLISH), results[i]));
        }

        System.out.println("\nNumber of skipped events is " + nbrOfSkippedEvents);
        System.out.println("Number of failed events is " + failedEvents.size());
    }

    // Just a temporary solution as no proper message queue was setup, as a result we are not aware of the event type.
    // With a proper message queue, instanceOf would be a better approach for getting event type and this method could
    // be combined with toEvent() for deserializing
    private static EventType getEventType(String input) {
        if (input.contains(EventType.InvoiceRegistered.toString())) {
            return EventType.InvoiceRegistered;
        }

        if (input.contains(EventType.PaymentRegistered.toString())) {
            return EventType.PaymentRegistered;
        }

        if (input.contains(EventType.LateFeeRegistered.toString())) {
            return EventType.LateFeeRegistered;
        }

        return EventType.ValueAdded;
    }

    private static void handle(Event event) {
        if (LocalDateTime.parse(event.timestamp).toLocalDate().getYear() == YEAR) {
            int index = LocalDateTime.parse(event.timestamp).toLocalDate().getDayOfYear();
            if (event.eventType == EventType.InvoiceRegistered || event.eventType == EventType.LateFeeRegistered) {
                dailyChanges[index - 1] = dailyChanges[index - 1] + event.amount;
            } else if (event.eventType == EventType.PaymentRegistered) {
                dailyChanges[index - 1] = dailyChanges[index - 1] - event.amount;
            }
        }
    }

    private static Event toEvent(String input, EventType type) throws IOException {
        if (type == EventType.InvoiceRegistered) {
            objectMapper.readValue(input, InvoiceRegisteredEvent.class);
        } else if (type == EventType.PaymentRegistered) {
            return objectMapper.readValue(input, PaymentRegisteredEvent.class);
        }

        return objectMapper.readValue(input, LateFeeRegisteredEvent.class);
    }

    private static double[] getMonthlyMaximumExposure(List<Pair<LocalDate, Double>> exposures) {
        double[] results = new double[12];
        int index = 0;
        while (index < exposures.size()) {
            LocalDate date = exposures.get(index).getKey();
            double exposure = exposures.get(index).getValue();
            System.out.println(date.toString() + " " + exposure);
            int monthIndex = date.getMonthValue() - 1;
            if (date.getDayOfMonth() == 1) {
                results[monthIndex] = exposure;
            }

            if (exposure > results[monthIndex]) {
                results[monthIndex] = exposure;
            }

            index++;
        }

        return results;
    }

    private static List<Pair<LocalDate, Double>> accumulator() {
        List<Pair<LocalDate, Double>> accumulated = new ArrayList<>();
        accumulated.add(new Pair<LocalDate, Double>(Year.of(YEAR).atDay( 1 ), dailyChanges[0]));
        for(int i = 1; i < dailyChanges.length; i++) {
            accumulated.add(new Pair<LocalDate, Double>(Year.of(YEAR).atDay(i + 1), accumulated.get(i - 1).getValue() + dailyChanges[i]));
        }
        return accumulated;
    }
}
