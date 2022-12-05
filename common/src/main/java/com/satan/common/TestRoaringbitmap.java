package com.satan.common;

import org.roaringbitmap.RoaringBitmap;

/**
 * @author liuwenyi
 * @date 2022/12/2
 **/
public class TestRoaringbitmap {
    public static void main(String[] args) {
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(1, 2, 3);
        for (int i = 4; i < 100; i++) {
            bitmap.add(i);
        }

        System.out.println(bitmap.contains(101));
        System.out.println(bitmap.select(1000));

    }
}
