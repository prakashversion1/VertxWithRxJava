/*
 * Copyright 2013 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 *
 */
package com.mycompany.myproject;


import io.vertx.rxcore.RxSupport;
import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;
import rx.Observable;

public class PingVerticle extends Verticle {

    public void start() {

        // load the general config object, loaded by using -config on command line
        JsonObject appConfig = container.config();

        // deploy the mongo-persistor module, which we'll use for persistence
        container.deployModule("io.vertx~mod-mongo-persistor~2.1.0", appConfig.getObject("mongo-persistor"));

        System.out.println("Successfully deployed modngo module");

        // setup Routematcher
        RouteMatcher matcher = new RouteMatcher();

        // the matcher for the complete list and the search
        matcher.get("/zips", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest req) {

                JsonObject json = new JsonObject();
                MultiMap params = req.params();

                if (params.size() > 0 && params.contains("state") || params.contains("city")) {
                    // create the matcher configuration
                    JsonObject matcher = new JsonObject();
                    if (params.contains("state")) matcher.putString("state", params.get("state"));
                    if (params.contains("city")) matcher.putString("city", params.get("city"));

                    // create the message for the mongo-persistor verticle
                    json = new JsonObject().putString("collection", "zips")
                            .putString("action", "find")
                            .putObject("matcher", matcher);

                } else {
                    // create the query
                    json = new JsonObject().putString("collection", "zips")
                            .putString("action", "find")
                            .putObject("matcher", new JsonObject());
                }

                JsonObject data = new JsonObject();
                data.putArray("results", new JsonArray());
                // and call the event we want to use
                vertx.eventBus().send("mongodb-persistor", json, new ReplyHandler(req, data));
            }
        });

        // the matcher for a specific id
        matcher.get("/zips/:id", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest req) {
                String idToRetrieve = req.params().get("id");

                // create the query
                JsonObject matcher = new JsonObject().putString("_id", idToRetrieve);
                JsonObject json = new JsonObject().putString("collection", "zips")
                        .putString("action", "find")
                        .putObject("matcher", matcher);

                // and call the event we want to use
                vertx.eventBus().send("mongodb-persistor", json, new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> event) {
                        req.response().putHeader("Content-Type", "application/json");
                        if (event.body().getArray("results").size() > 0) {
                            JsonObject result = event.body().getArray("results").get(0);
                            req.response().end(result.encodePrettily());
                        }
                    }
                });
            }
        });

        // the matcher for the update
        matcher.post("/zips/:id", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest req) {

                // process the body
                req.bodyHandler(new Handler<Buffer>() {

                    @Override
                    public void handle(Buffer event) {
                        // normally we'd validate the input, for now just assume it is correct.
                        final String body = event.getString(0, event.length());

                        // create the query
                        JsonObject newObject = new JsonObject(body);
                        JsonObject matcher = new JsonObject().putString("_id", req.params().get("id"));
                        JsonObject json = new JsonObject().putString("collection", "zips")
                                .putString("action", "update")
                                .putObject("criteria", matcher)
                                .putBoolean("upsert", false)
                                .putBoolean("multi", false)
                                .putObject("objNew", newObject);

                        // and call the event we want to use
                        vertx.eventBus().send("mongodb-persistor", json, new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> event) {
                                // we could handle the errors here, but for now
                                // assume everything went ok, and return the original
                                // and updated json
                                req.response().end(body);
                            }
                        });
                    }
                });
            }
        });

        matcher.post("/rxzips/:id", new Handler<HttpServerRequest>() {

            public void handle(final HttpServerRequest request) {
                Observable<Buffer> requestObservable = RxSupport.toObservable(request);

            }
        });

        // create and run the server
        HttpServer server = vertx.createHttpServer().requestHandler(matcher).listen(8080);

        // output that the server is started
        System.out.println("Webserver started, listening on port: 8080");
    }


    /**
     * Simple handler that can be used to handle the reply from mongodb-persistor
     * and handles the 'more-exist' field.
     */
    private static class ReplyHandler implements Handler<Message<JsonObject>> {

        private final HttpServerRequest request;
        private JsonObject data;

        private ReplyHandler(final HttpServerRequest request, JsonObject data) {
            this.request = request;
            this.data = data;
        }

        @Override
        public void handle(Message<JsonObject> event) {
            // if the response contains more message, we need to get the rest
            if (event.body().getString("status").equals("more-exist")) {
                JsonArray results = event.body().getArray("results");

                for (Object el : results) {
                    data.getArray("results").add(el);
                }

                event.reply(new JsonObject(), new ReplyHandler(request, data));
            } else {

                JsonArray results = event.body().getArray("results");
                for (Object el : results) {
                    data.getArray("results").add(el);
                }

                request.response().putHeader("Content-Type", "application/json");
                request.response().end(data.encodePrettily());
            }
        }
    }
}