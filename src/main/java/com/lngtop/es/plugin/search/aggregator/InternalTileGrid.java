/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.lngtop.es.plugin.search.aggregator;

import com.lngtop.es.plugin.utils.GeoUtils;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.*;

/**
 * Represents a grid of cells where each cell's location is determined by a tile.
 * All tile in a grid are of the same precision and held internally as a single long
 * for efficiency's sake.
 */
public class InternalTileGrid extends InternalMultiBucketAggregation<InternalTileGrid, InternalTileGrid.Bucket> implements
        TileGrid {

    public static final Type TYPE = new Type("tile_grid");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalTileGrid readResult(StreamInput in) throws IOException {
            InternalTileGrid buckets = new InternalTileGrid();
            buckets.readFrom(in);
            return buckets;
        }
    };


    public static final BucketStreams.Stream<Bucket> BUCKET_STREAM = new BucketStreams.Stream<Bucket>() {
        @Override
        public Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            Bucket bucket = new Bucket();
            bucket.readFrom(in);
            return bucket;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            return context;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }


    static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements TileGrid.Bucket, Comparable<Bucket> {

        protected long tileId;
        protected long docCount;
        protected InternalAggregations aggregations;

        public Bucket() {
            // For Serialization only
        }

        public Bucket(long tileId, long docCount, InternalAggregations aggregations) {
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.tileId = tileId;
        }

        @Override
        public String getKeyAsString() {
            return GeoUtils.stringEncode(tileId);
        }

        @Override
        public GeoPoint getKey() {
            return GeoPoint.fromGeohash(tileId);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public int compareTo(Bucket other) {
            if (this.tileId > other.tileId) {
                return 1;
            }
            if (this.tileId < other.tileId) {
                return -1;
            }
            return 0;
        }

        public Bucket reduce(List<? extends Bucket> buckets, ReduceContext context) {
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            long docCount = 0;
            for (Bucket bucket : buckets) {
                docCount += bucket.docCount;
                aggregationsList.add(bucket.aggregations);
            }
            final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
            return new Bucket(tileId, docCount, aggs);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            tileId = in.readLong();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(tileId);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY, getKeyAsString());
            builder.field(CommonFields.DOC_COUNT, docCount);
            double[] bounds = GeoUtils.gridToLatLong(getKeyAsString());
            builder.field(new XContentBuilderString("bounds"), bounds);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }
    }
    private int requiredSize;
    private Collection<Bucket> buckets;
    protected Map<String, Bucket> bucketMap;

    InternalTileGrid() {
    } // for serialization

    public InternalTileGrid(String name, int requiredSize, Collection<Bucket> buckets, List<PipelineAggregator> pipelineAggregators,
                            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.requiredSize = requiredSize;
        this.buckets = buckets;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalTileGrid create(List<Bucket> buckets) {
        return new InternalTileGrid(this.name, this.requiredSize, buckets, this.pipelineAggregators(), this.metaData);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.tileId, prototype.docCount, aggregations);
    }

    @Override
    public List<TileGrid.Bucket> getBuckets() {
        Object o = buckets;
        return (List<TileGrid.Bucket>) o;
    }

    @Override
    public InternalTileGrid doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {

        LongObjectPagedHashMap<List<Bucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalTileGrid grid = (InternalTileGrid) aggregation;
            if (buckets == null) {
                buckets = new LongObjectPagedHashMap<>(grid.buckets.size(), reduceContext.bigArrays());
            }
            for (Bucket bucket : grid.buckets) {
                List<Bucket> existingBuckets = buckets.get(bucket.tileId);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.tileId, existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        final int size = (int) Math.min(requiredSize, buckets.size());
        BucketPriorityQueue ordered = new BucketPriorityQueue(size);
        for (LongObjectPagedHashMap.Cursor<List<Bucket>> cursor : buckets) {
            List<Bucket> sameCellBuckets = cursor.value;
            ordered.insertWithOverflow(sameCellBuckets.get(0).reduce(sameCellBuckets, reduceContext));
        }
        buckets.close();
        Bucket[] list = new Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }
        return new InternalTileGrid(getName(), requiredSize, Arrays.asList(list), pipelineAggregators(), getMetaData());
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        this.requiredSize = readSize(in);
        int size = in.readVInt();
        List<Bucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Bucket bucket = new Bucket();
            bucket.readFrom(in);
            buckets.add(bucket);
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeSize(requiredSize, out);
        out.writeVInt(buckets.size());
        for (Bucket bucket : buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS);
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    static class BucketPriorityQueue extends PriorityQueue<Bucket> {

        public BucketPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(Bucket o1, Bucket o2) {
            long i = o2.getDocCount() - o1.getDocCount();
            if (i == 0) {
                i = o2.compareTo(o1);
                if (i == 0) {
                    i = System.identityHashCode(o2) - System.identityHashCode(o1);
                }
            }
            return i > 0;
        }
    }
}
