package es.upm.cloud.flink.loadGenerator;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * @author Ainhoa Azqueta Alzúaz (aazqueta@fi.upm.es)
 * @organization Universidad Politécnica de Madrid
 * @laboratory Laboratorio de Sistemas Distributidos (LSD)
 * @date 11/11/24
 **/
public class TemperatureSource {
    private static final int PORT = 9999;


    public static void main(String[] args) {
        final Random random = new Random();
        final Integer[] machineIds = {1,2,3};
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server started on port " + PORT);

            try (Socket clientSocket = serverSocket.accept();
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
                 System.out.println("Client connected");
                while (true) {
                    // Simulate sending events
                    // Choose a machine id
                    Integer machineId = machineIds[random.nextInt(machineIds.length)];

                    // Generate random temperature between 20 and 100 degrees
                    double temperature = 20 + (80 * random.nextDouble());

                    // Generate an actual timestamp in ms
                    long timestamp = System.currentTimeMillis();

                    // Generate a late event time to time
                    if (random.nextDouble() < 0.1) { // 10% probability
                        timestamp -= random.nextInt(30000); // 5 seconds late
                        System.out.println("LateEvent..");
                    }

                    // Crear el evento de temperatura
                    TemperatureEvent event = new TemperatureEvent(machineId, temperature, timestamp);
                    out.println(event);
                    System.out.println("Sent: " + event);

                    // Esperar un tiempo aleatorio entre 500 y 1000 ms antes de emitir el siguiente evento
                    //Thread.sleep(500 + random.nextInt(500));
                    Thread.sleep(1000);
                }

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class TemperatureEvent {
        private int machineId;
        private double temperature;
        private long timestamp;

        public TemperatureEvent(int machineId, double temperature, long timestamp) {
            this.machineId = machineId;
            this.temperature = temperature;
            this.timestamp = timestamp;
        }

        public int getMachineId() {
            return machineId;
        }

        public double getTemperature() {
            return temperature;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "{" +
                    "\"machineId\":" + machineId +
                    ", \"temperature\":" + temperature +
                    ", \"timestamp\":" + timestamp +
                    '}';
        }
    }
}