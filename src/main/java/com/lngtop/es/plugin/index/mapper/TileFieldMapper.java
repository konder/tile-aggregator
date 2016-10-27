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

package com.lngtop.es.plugin.index.mapper;

import com.google.common.collect.Iterators;
import com.lngtop.es.plugin.utils.GeoUtils;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.ArrayValueMapperParser;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.index.mapper.MapperBuilders.doubleField;
import static org.elasticsearch.index.mapper.MapperBuilders.stringField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;

public class TileFieldMapper extends FieldMapper implements ArrayValueMapperParser {
    private static ESLogger logger = ESLoggerFactory.getLogger("mapper.tile");
    protected static final DeprecationLogger deprecationLogger = new DeprecationLogger(Loggers.getLogger(TileFieldMapper.class));

    public static final String CONTENT_TYPE = "geo_tile";

    public static class Names {
        public static final String LAT = "lat";
        public static final String LON = "lon";
        public static final String TILE = "tileid";
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    public static class Defaults {
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
        public static final boolean ENABLE_LATLON = false;
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);

        public static final GeoTileFieldType FIELD_TYPE = new GeoTileFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.freeze();
        }
    }

    private DoubleFieldMapper latMapper;

    private DoubleFieldMapper lonMapper;

    private final ContentPath.Type pathType;

    private StringFieldMapper tileMapper;

    public TileFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings,
                           ContentPath.Type pathType, DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper,
                           StringFieldMapper tileMapper, MultiFields multiFields, Explicit<Boolean> ignoreMalformed, CopyTo copyTo) {

        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.pathType = pathType;
        this.latMapper = latMapper;
        this.lonMapper = lonMapper;
        this.tileMapper = tileMapper;
    }

    @Override
    public TileFieldMapper.GeoTileFieldType fieldType() {
        return (TileFieldMapper.GeoTileFieldType) super.fieldType();
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> extras = new ArrayList<>();
        if (fieldType().isLatLonEnabled()) {
            extras.add(latMapper);
            extras.add(lonMapper);
        }
        extras.add(tileMapper);
        return Iterators.concat(super.iterator(), extras.iterator());
    }

    public static class GeoTileFieldType extends MappedFieldType {
        protected MappedFieldType tileFieldType;

        protected MappedFieldType latFieldType;
        protected MappedFieldType lonFieldType;

        GeoTileFieldType() {
        }

        GeoTileFieldType(TileFieldMapper.GeoTileFieldType ref) {
            super(ref);
            this.tileFieldType = ref.tileFieldType; // copying ref is ok, this can never be modified
            this.latFieldType = ref.latFieldType; // copying ref is ok, this can never be modified
            this.lonFieldType = ref.lonFieldType; // copying ref is ok, this can never be modified
        }

        @Override
        public MappedFieldType clone() {
            return new TileFieldMapper.GeoTileFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            TileFieldMapper.GeoTileFieldType that = (TileFieldMapper.GeoTileFieldType) o;
            return java.util.Objects.equals(tileFieldType, that.tileFieldType) &&
                    java.util.Objects.equals(latFieldType, that.latFieldType) &&
                    java.util.Objects.equals(lonFieldType, that.lonFieldType);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(super.hashCode(), tileFieldType, latFieldType,
                    lonFieldType);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public MappedFieldType tileFieldType() {
            return tileFieldType;
        }

        public boolean isLatLonEnabled() {
            return latFieldType != null;
        }

        public MappedFieldType latFieldType() {
            return latFieldType;
        }

        public MappedFieldType lonFieldType() {
            return lonFieldType;
        }

        public void setLatLonEnabled(MappedFieldType latFieldType, MappedFieldType lonFieldType) {
            checkIfFrozen();
            this.latFieldType = latFieldType;
            this.lonFieldType = lonFieldType;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            TileFieldMapper.Builder builder = new TileFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("lat_lon")) {
                    deprecationLogger.deprecated(CONTENT_TYPE + " lat_lon parameter is deprecated and will be removed "
                            + "in the next major release");
                    builder.enableLatLon(XContentMapValues.nodeBooleanValue(propNode));
                    iterator.remove();
                } else if (propName.equals(TileFieldMapper.Names.IGNORE_MALFORMED)) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode));
                    iterator.remove();
                } else if (parseMultiField(builder, name, parserContext, propName, propNode)) {
                    iterator.remove();
                }
            }

            return builder;
        }
    }


    public static class Builder extends FieldMapper.Builder<TileFieldMapper.Builder, TileFieldMapper> {
        protected ContentPath.Type pathType = TileFieldMapper.Defaults.PATH_TYPE;

        protected boolean enableLatLon = TileFieldMapper.Defaults.ENABLE_LATLON;

        protected Boolean ignoreMalformed;

        public Builder(String name) {
            super(name, TileFieldMapper.Defaults.FIELD_TYPE, TileFieldMapper.Defaults.FIELD_TYPE);
            this.builder = this;
        }

        @Override
        public TileFieldMapper.GeoTileFieldType fieldType() {
            return (TileFieldMapper.GeoTileFieldType) fieldType;
        }

        @Override
        public TileFieldMapper.Builder multiFieldPathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return builder;
        }

        @Override
        public TileFieldMapper.Builder fieldDataSettings(Settings settings) {
            this.fieldDataSettings = settings;
            return builder;
        }

        public TileFieldMapper.Builder enableLatLon(boolean enableLatLon) {
            this.enableLatLon = enableLatLon;
            return builder;
        }

        public TileFieldMapper.Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(context.indexSettings().getAsBoolean("index.mapping.ignore_malformed", TileFieldMapper.Defaults.IGNORE_MALFORMED.value()), false);
            }
            return TileFieldMapper.Defaults.IGNORE_MALFORMED;
        }

        public TileFieldMapper build(BuilderContext context, String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                     Settings indexSettings, ContentPath.Type pathType, DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper,
                                     StringFieldMapper tileMapper, MultiFields multiFields, Explicit<Boolean> ignoreMalformed, CopyTo copyTo) {
            fieldType.setTokenized(false);
            if (context.indexCreatedVersion().before(Version.V_2_3_0)) {
                fieldType.setNumericPrecisionStep(GeoPointField.PRECISION_STEP);
                fieldType.setNumericType(FieldType.NumericType.LONG);
            }
            setupFieldType(context);
            return new TileFieldMapper(simpleName, fieldType, defaultFieldType, indexSettings, pathType, latMapper, lonMapper,
                    tileMapper, multiFields, ignoreMalformed, copyTo);
        }

        public TileFieldMapper build(Mapper.BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            TileFieldMapper.GeoTileFieldType geoPointFieldType = (TileFieldMapper.GeoTileFieldType) fieldType;

            DoubleFieldMapper latMapper = null;
            DoubleFieldMapper lonMapper = null;

            context.path().add(name);
            if (enableLatLon) {
                NumberFieldMapper.Builder<?, ?> latMapperBuilder = doubleField(TileFieldMapper.Names.LAT).includeInAll(false);
                NumberFieldMapper.Builder<?, ?> lonMapperBuilder = doubleField(TileFieldMapper.Names.LON).includeInAll(false);
                latMapper = (DoubleFieldMapper) latMapperBuilder.includeInAll(false).store(fieldType.stored()).docValues(false).build(context);
                lonMapper = (DoubleFieldMapper) lonMapperBuilder.includeInAll(false).store(fieldType.stored()).docValues(false).build(context);
                geoPointFieldType.setLatLonEnabled(latMapper.fieldType(), lonMapper.fieldType());
            }
            StringFieldMapper tileMapper = stringField(TileFieldMapper.Names.TILE).index(true).tokenized(false).includeInAll(false).store(true)
                    .omitNorms(true).indexOptions(IndexOptions.DOCS).build(context);

            context.path().remove();
            context.path().pathType(origPathType);

            return build(context, name, fieldType, defaultFieldType, context.indexSettings(), origPathType,
                    latMapper, lonMapper, tileMapper, multiFieldsBuilder.build(this, context), ignoreMalformed(context), copyTo);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {

        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);
        context.path().add(simpleName());

        GeoPoint sparse = context.parseExternalValue(GeoPoint.class);

        if (sparse != null) {
            parse(context, sparse, null);
        } else {
            sparse = new GeoPoint();
            XContentParser.Token token = context.parser().currentToken();
            if (token == XContentParser.Token.START_ARRAY) {
                token = context.parser().nextToken();
                if (token == XContentParser.Token.START_ARRAY) {
                    // its an array of array of lon/lat [ [1.2, 1.3], [1.4, 1.5] ]
                    while (token != XContentParser.Token.END_ARRAY) {
                        parse(context, org.elasticsearch.common.geo.GeoUtils.parseGeoPoint(context.parser(), sparse), null);
                        token = context.parser().nextToken();
                    }
                } else {
                    // its an array of other possible values
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        double lon = context.parser().doubleValue();
                        token = context.parser().nextToken();
                        double lat = context.parser().doubleValue();
                        while ((token = context.parser().nextToken()) != XContentParser.Token.END_ARRAY);
                        parse(context, sparse.reset(lat, lon), null);
                    } else {
                        while (token != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                parsePointFromString(context, sparse, context.parser().text());
                            } else {
                                parse(context, org.elasticsearch.common.geo.GeoUtils.parseGeoPoint(context.parser(), sparse), null);
                            }
                            token = context.parser().nextToken();
                        }
                    }
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                parsePointFromString(context, sparse, context.parser().text());
            } else if (token != XContentParser.Token.VALUE_NULL) {
                parse(context, org.elasticsearch.common.geo.GeoUtils.parseGeoPoint(context.parser(), sparse), null);
            }
        }

        context.path().remove();
        context.path().pathType(origPathType);
        return null;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || pathType != TileFieldMapper.Defaults.PATH_TYPE) {
            builder.field("path", pathType.name().toLowerCase(Locale.ROOT));
        }
        if (includeDefaults ) {
            builder.field("lat_lon", true);
            builder.field("tileid", true);
        }
    }

    @Override
    public FieldMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        TileFieldMapper updated = (TileFieldMapper) super.updateFieldType(fullNameToFieldType);
        StringFieldMapper geoUpdated = tileMapper == null ? null : (StringFieldMapper) tileMapper.updateFieldType(fullNameToFieldType);
        DoubleFieldMapper latUpdated = latMapper == null ? null : (DoubleFieldMapper) latMapper.updateFieldType(fullNameToFieldType);
        DoubleFieldMapper lonUpdated = lonMapper == null ? null : (DoubleFieldMapper) lonMapper.updateFieldType(fullNameToFieldType);
        if (updated == this
                && geoUpdated == tileMapper
                && latUpdated == latMapper
                && lonUpdated == lonMapper) {
            return this;
        }
        if (updated == this) {
            updated = (TileFieldMapper) updated.clone();
        }
        updated.tileMapper = geoUpdated;
        updated.latMapper = latUpdated;
        updated.lonMapper = lonUpdated;
        return updated;
    }

    private void parse(ParseContext context, GeoPoint point, String tile) throws IOException {
        if (point.lat() > 90.0 || point.lat() < -90.0) {
            throw new IllegalArgumentException("illegal latitude value [" + point.lat() + "] for " + name());
        }
        if (point.lon() > 180.0 || point.lon() < -180) {
            throw new IllegalArgumentException("illegal longitude value [" + point.lon() + "] for " + name());
        }
        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            context.doc().add(new GeoPointField(fieldType().names().indexName(), point.lon(), point.lat(), fieldType()));
        }
        if (tile == null) {
            tile = GeoUtils.latLongToGrid(point.lat(), point.lon());
        }
        tileMapper.parse(context.createExternalValueContext(tile));

        if (fieldType().isLatLonEnabled()) {
            latMapper.parse(context.createExternalValueContext(point.lat()));
            lonMapper.parse(context.createExternalValueContext(point.lon()));
        }
        multiFields.parse(this, context.createExternalValueContext(point));
    }

    private void parsePointFromString(ParseContext context, GeoPoint sparse, String point) throws IOException {
        if (point.indexOf(',') < 0) {
            parse(context, sparse.resetFromGeoHash(point), point);
        } else {
            parse(context, sparse.resetFromString(point), null);
        }
    }
}
