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


package com.lngtop.es.plugin.utils;

import java.util.Stack;

/**
 * Created by zhangnan on 16/5/6.
 */
public class GeoUtils {
    private static final double PI = 3.1415926;
    private static final int PixelsPerTile = 256;
    private static final double MinLatitude = -85.0511287798;
    private static final double MaxLatitude = 85.0511287798;
    private static final int MinLongitude = -180;
    private static final int MaxLongitude = 180;
    private static final int EarthRadiusInMeters = 6378137;
    private static final double EarthCircumferenceInMeters = 2 * PI * EarthRadiusInMeters;

    private static double clip(double n, double minValue, double maxValue) {
        return Math.min(Math.max(n, minValue), maxValue);
    }

    public static String latLongToGrid(double latitude, double longitude) {
        return latLongToGrid(latitude, longitude, 20);
    }

    public static String latLongToGrid(double latitude, double longitude, int levelOfDetail) {
        // 1. LatLongToPixels
        latitude = clip(latitude, MinLatitude, MaxLatitude) * PI / 180;
        longitude = clip(longitude, MinLongitude, MaxLongitude) * PI / 180;
        double sinLatitude = Math.sin(latitude);
        double xMeters = EarthRadiusInMeters * longitude;
        double lLog = Math.log((1 + sinLatitude) / (1 - sinLatitude));
        double yMeters = EarthRadiusInMeters / 2 * lLog;
        long numPixels = (long) PixelsPerTile << levelOfDetail;
        double metersPerPixel = EarthCircumferenceInMeters / numPixels;

        long xPixel = (long) clip((double) (EarthCircumferenceInMeters / 2 + xMeters) / metersPerPixel + 0.5, 0, numPixels - 1);
        double tmp = EarthCircumferenceInMeters / 2 - yMeters;
        long yPixel = (long) clip((double) tmp / metersPerPixel + 0.5, 0, numPixels - 1);

        // 2. PixelToGrid
        StringBuffer pGridName = new StringBuffer(levelOfDetail);

        int xmin = 0;
        int xmax = 0;
        int ymin = 0;
        int ymax = 0;

        xmax = ymax = (PixelsPerTile << (levelOfDetail));

        int gn = 0;
        for (int i = 0; i < levelOfDetail; i++) {
            gn = 0;
            int cx = ((xmin + xmax) >> 1);
            int cy = ((ymin + ymax) >> 1);
            if (xPixel > cx) {
                gn = 1;
                xmin = cx;
            } else {
                gn = 0;
                xmax = cx;
            }
            if (yPixel > cy) {
                gn += 2;
                ymin = cy;
            } else {
                gn += 0;
                ymax = cy;
            }
            pGridName.append(String.valueOf(gn));
        }

        return new String(pGridName);
    }

    public static long longEncode(String str) {
        long val = 0;
        char[] arr$ = str.toCharArray();
        int len$ = arr$.length;


        for (int i$ = 0; i$ < len$; ++i$) {
            char c = arr$[i$];
            val += Integer.parseInt(String.valueOf(c)) * Math.pow(4, (len$ - i$ - 1));
        }
        return val;
    }

    private final static String DIGTHS = "0123";

    public static String stringEncode(long num) {
        StringBuffer str = new StringBuffer("");
        Stack<Character> s = new Stack<Character>();
        while (num != 0) {
            s.push(DIGTHS.charAt((int) (num % 4)));
            num /= 4;
        }
        while (!s.isEmpty()) {
            str.append(s.pop());
        }
        return str.toString();
    }


    public static double[] gridToLatLong(String tileId) {
        int levelOfDetail = tileId.length();
        long xmin, xmax, ymin, ymax;
        xmin = ymin = 0;
        xmax = ymax = (PixelsPerTile << (levelOfDetail));

        for (int i = 0; i < levelOfDetail; i++) {
            int gridIndexNum = tileId.charAt(i);
            if ((gridIndexNum & 1) > 0) {
                xmin = ((xmax + xmin) >> 1);
            } else {
                xmax = ((xmax + xmin) >> 1);
            }
            if ((gridIndexNum & 2) > 0) {
                ymin = ((ymax + ymin) >> 1);
            } else {
                ymax = ((ymax + ymin) >> 1);
            }
        }

        double[] bounds = new double[4];
        double[] ne = pixelToLatLong(xmax, ymax, levelOfDetail);
        double[] sw = pixelToLatLong(xmin, ymin, levelOfDetail);
        System.arraycopy(ne, 0, bounds, 0, 2);
        System.arraycopy(sw, 0, bounds, 2, 2);
        return bounds;
    }

    private static double[] pixelToLatLong(long xPixel, long yPixel,
                                          int levelOfDetail) {
        if (xPixel < 0 || yPixel < 0 || levelOfDetail < 0 || PixelsPerTile <= 0) {
            return new double[]{0, 0};
        }
        double y, x;
        double fd = 40075016.685578488D / (double) ((1 << levelOfDetail) * PixelsPerTile);
        double ia = (double) xPixel * fd - 20037508.342789244D;
        double hT = 20037508.342789244D - (double) yPixel * fd;
        y = 1.5707963267948966D - 2D * Math.atan(Math.exp(-hT / 6378137D));
        y *= 57.295779513082323D;
        x = ia / 6378137D;
        x *= 57.295779513082323D;
        return new double[]{x, y};
    }

    public static void main(String args[]) {
        String tileId = latLongToGrid(39.8775, 116.316, 20);
        //System.out.println(tileId);
        //System.out.println(longEncode(tileId));
        //System.out.println(stringEncode(longEncode(tileId)));

        gridToLatLong(tileId);
    }

    private static double Clip(double d, double d1, double d2) {
        if (d < d1)
            return (d1);

        if (d > d2)
            return (d2);

        return (d);
    }
}
