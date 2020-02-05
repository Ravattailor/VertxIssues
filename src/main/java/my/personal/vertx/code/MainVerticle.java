package my.personal.vertx.code;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.servicediscovery.ServiceReference;
import io.vertx.servicediscovery.types.HttpEndpoint;

public class MainVerticle extends AbstractVerticle {

  ServiceDiscovery discovery;


  @Override
  public void start(Promise<Void> startPromise) {
    discovery = ServiceDiscovery.create(vertx,
      new ServiceDiscoveryOptions()
        .setAnnounceAddress("service-announce")
        .setName("my-name"));

    discovery.close();
    // Manual record creation
    Record record = new Record()
      .setType("eventbus-service-proxy")
      .setLocation(new JsonObject().put("endpoint", "the-service-address"))
      .setName("my-service")
      .setMetadata(new JsonObject().put("some-label", "some-value"));

    discovery.publish(record, ar -> {
      if (ar.succeeded()) {
        System.out.println("publication succeeded" + ar.result());
        Record publishedRecord = ar.result();
      } else {
        System.out.println("publication failed" + ar.cause());
      }
    });


    // Record creation from a type
    record = HttpEndpoint.createRecord("some-rest-api", "localhost", 8080, "/api");
    discovery.publish(record, ar -> {
      if (ar.succeeded()) {
        System.out.println("publication succeeded" + ar.result());
        Record publishedRecord = ar.result();
      } else {
        System.out.println("publication failed" + ar.cause());
      }
    });

    discovery.getRecord(r -> r.getName().equals("my-service"), ar -> {
      if (ar.succeeded()) {
        if (ar.result() != null) {
          System.out.println("we have a record");
          System.out.println("record " + ar.result());

          ServiceReference reference = discovery.getReference(ar.result());
          // Retrieve the service object
          MessageConsumer<JsonObject> consumer = reference.getAs(MessageConsumer.class);

          // Attach a message handler on it
          consumer.handler(message -> {
            System.out.println(" message handler");
            JsonObject payload = message.body();
            System.out.println("payload");
          });

        } else {
          System.out.println("the lookup succeeded, but no matching service" + ar.cause());
        }
      } else {

        System.out.println("lookup failed" + ar.cause());
      }
    });


    startPromise.complete();
  }

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    Promise<String> promise = Promise.promise();
    vertx.deployVerticle(MainVerticle.class.getName(), promise);
    promise.future().setHandler(id -> {
      System.out.println("verticle deployed successfully");
    });
  }
}
