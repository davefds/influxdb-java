package org.influxdb.dto;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import retrofit.client.Header;
import retrofit.client.Response;

/**
     * A Streaming (or Chunked) response is one where the server sends query results as
     * they are available.  They are returned in the time-order they are requested in, but
     * the stream may include a mix of multiple Series.
     *
     * https://docs.influxdata.com/influxdb/v0.8/api/chunked_responses/
     *
     * { "name":"testSeries","columns":["time","sequence_number","value2"],
     *   "points":[[1453521282351,174844500001,5],[1453521282350,174844490001,5], ... ] }
     *
     */
    public class ChunkedResponse implements AutoCloseable {

        private final long queryStartTime;
        private final String database;
        private final String query;
        private final TimeUnit precision;

        private final Response response;

        private final JsonReader reader;
        private final boolean chunked;
        private boolean endOfStream = false;

        public ChunkedResponse( long queryStartTime, String database, String query, TimeUnit precision,
                                Response response ) throws IOException {
            super();
            this.queryStartTime = queryStartTime;
            this.database = database;
            this.query = query;
            this.precision = precision;
            this.response = response;

            chunked = response.getHeaders()
                              .stream()
                              .filter( (h) -> {
                                  return h.getName().startsWith("Transfer-Encoding") &&
                                          h.getValue().equals( "chunked" );
                              } )
                              .count() > 0;

            if ( ! chunked ) {
                throw new IllegalStateException("Response is not chunked.");
            }

            if ( ! response.getBody().mimeType().equals( "application/json" ) ) {
                throw new IllegalStateException("Chunked response handling only supports application/json mime type response.");
            }

            reader = new JsonReader( new InputStreamReader(response.getBody().in()) );
            reader.setLenient( true );
        }

        /**
         * Check if the end of stream has been reached.
         *
         * @return true if end of stream has been reached and further chunk requests will return null
         */
        public boolean isEndOfStream() {
            return endOfStream;
        }

        /**
         *
         * @return the query start timestamp
         */
        public long getQueryStartTime() {
            return queryStartTime;
        }

        /**
         *
         * @return the database the query was executed against
         */
        public String getDatabase() {
            return database;
        }

        /**
         *
         * @return the query string
         */
        public String getQuery() {
            return query;
        }

        /**
         *
         * @return the query time precision
         */
        public TimeUnit getPrecision() {
            return precision;
        }

        /**
         *
         * @return true if the response is chunked (Transfer-Encoding: chunked)
         */
        public boolean isChunked() {
            return chunked;
        }

        /**
         *
         * @return the next chunk available from the response stream
         */
        public Serie nextChunk() {
            try {
                if (endOfStream || reader.peek().equals( JsonToken.END_DOCUMENT )) {
                    return null;
                }

                reader.beginObject();
                if ( !(reader.hasNext() && reader.nextName().equals( "name" )) )
                    throw new IllegalStateException("Incomplete stream. Unable to read series name.");

                Serie.Builder serieBuilder = new Serie.Builder( reader.nextString() );

                if ( !(reader.hasNext() && reader.nextName().equals( "columns" )) )
                    throw new IllegalStateException("Incomplete stream. Unable to read series columns.");

                List<String> columns = new ArrayList<>();
                reader.beginArray();
                while (reader.hasNext()) {
                    columns.add( reader.nextString() );
                    if (reader.peek().equals( JsonToken.END_ARRAY ) )
                        break;
                }
                reader.endArray();
                serieBuilder.columns( columns.toArray( new String[columns.size()] ) );

                if ( !(reader.hasNext() && reader.nextName().equals( "points" )) )
                    throw new IllegalStateException("Incomplete stream. Unable to read series points.");

                reader.beginArray();
loop:           do {

                    JsonToken token = reader.peek();
                    switch (token) {
                        case END_DOCUMENT:
                        case END_OBJECT:
                        case END_ARRAY:
                            break loop;

                        default:
                            break;
                    }
                    List<Object> row = readRow();
                    serieBuilder.values( row.toArray(  new Object[row.size()] ));

                } while (true);
                reader.endArray();

                JsonToken token = reader.peek();
                switch (token) {
                    case END_DOCUMENT:
                        endOfStream = true;
                        break;

                    case END_OBJECT:
                        reader.endObject();
                        token = reader.peek();
                        if (JsonToken.END_DOCUMENT.equals( token ))
                            endOfStream = true;
                        break;

                    default:
                        break;
                }

                return serieBuilder.build();

            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        private List<Object> readRow() throws IOException {

            List<Object> row = new ArrayList<>();
            int c = 0;

            reader.beginArray();
            while (reader.hasNext()) {

                JsonToken token = reader.peek();
                switch (token) {

                    case STRING:
                        row.add( reader.nextString() );
                        c++;
                        break;

                    case NUMBER:
                        // Each query returns the time and sequence_number as the
                        // first two columns
                        if (c < 2)
                            row.add( reader.nextLong() );
                        else
                            row.add( reader.nextDouble() );
                        c++;
                        break;

                    case END_DOCUMENT:
                        // really shouldn't happen... would break on end of array first.
                        endOfStream = true;
                        break;

                    case END_OBJECT:
                    default:
                        throw new IllegalStateException("Unexpected token type: " + token);
                }
            }
            reader.endArray();

            return row;
        }

        @Override
        public void close() throws Exception {
            try { if (reader != null) reader.close(); }
            catch (Exception ignoreOnClose) {}
        }

        /**
         * @return the url the query was executed against
         */
        public String getUrl() {
            return response.getUrl();
        }

        /**
         * @return the response status
         */
        public int getStatus() {
            return response.getStatus();
        }

        /**
         * @return the response reason
         */
        public String getReason() {
            return response.getReason();
        }

        /**
         * @return the response headers
         */
        public List<Header> getHeaders() {
            return new ArrayList<Header>(response.getHeaders());
        }

        /**
         * @return the response stream length, or -1 if not known
         */
        public long getResponseLength() {
            return response.getBody().length();
        }

        /**
         * @return the mime-type of the response
         */
        public String getMimeType() {
            return response.getBody().mimeType();
        }
    }