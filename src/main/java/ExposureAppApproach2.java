import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import model.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.*;
import java.time.format.TextStyle;
import java.util.*;

@Slf4j
public class ExposureAppApproach2 {
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final int YEAR = 2022;
    public static double current;
    public static double day;
    public static boolean waitingForFirstDayComplete = false;
    public static int month;
    public static double[] results = new double[12];
    public static int nbrOfSkippedEvents = 0;
    public static List<String> failedEvents = new ArrayList<>();

    public static void main (String[] args) throws IOException {
        log.info("Application started!");

        BufferedReader reader = new BufferedReader(new FileReader("/Users/yuchen/IdeaProjects/Invoier/src/main/resources/events.json"));
        String line;

        day = 1;
        month = 1;
        while ((line = reader.readLine()) != null) {
            process(line);
        }

        printResults(results);
    }

    private static void process(String line) {
        EventType type = getEventType(line);
        if (isApplicable(type)) {
            try {
                handle(toEvent(line, type));
            } catch (Exception e) {
                failedEvents.add(line);
                log.debug("Failed to process event!", line, e);
            }
        }
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
            // Is a new day
            if(day != LocalDateTime.parse(event.timestamp).toLocalDate().getDayOfYear()) {

                day = LocalDateTime.parse(event.timestamp).toLocalDate().getDayOfYear();
                int currentMonth = LocalDateTime.parse(event.timestamp).toLocalDate().getMonthValue();
                int currentMonthIndex = currentMonth - 1;
                int previousMonthIndex = currentMonth - 2;

                // Is still the same month
                if(month == LocalDateTime.parse(event.timestamp).toLocalDate().getMonthValue()) {
                    if(waitingForFirstDayComplete) {
                        // Initialize the exposure of this month to the exposure of first day
                        // (Mainly used for cases when exposure is negative)
                        results[currentMonthIndex] = current;
                        waitingForFirstDayComplete = false;
                        if (event.eventType == EventType.InvoiceRegistered || event.eventType == EventType.LateFeeRegistered) {
                            current = current + event.amount;
                        } else if (event.eventType == EventType.PaymentRegistered) {
                            current = current - event.amount;
                        }
                        return;
                    }

                    // Exposure of yesterday > the maxium exposure from previous days this month
                    if(current > results[currentMonthIndex]) {
                        results[currentMonthIndex] = current;
                    }
                } else {
                    // First day in a new month

                    // Handle cases when the last day of previous month has the highest exposure
                    if(current > results[previousMonthIndex]) {
                        results[previousMonthIndex] = current;
                    }

                    month = LocalDateTime.parse(event.timestamp).toLocalDate().getMonthValue();
                    waitingForFirstDayComplete = true;
                }
            }

            if (event.eventType == EventType.InvoiceRegistered || event.eventType == EventType.LateFeeRegistered) {
                current = current + event.amount;
            } else if (event.eventType == EventType.PaymentRegistered) {
                current = current - event.amount;
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
}
