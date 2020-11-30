package com.satan.hadoop.cf;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;

/**
 * @author liuwenyi
 * @date 2020/11/19
 */
public class Main {

    public static void main(String[] args) throws UnknownHostException, SocketException {
        System.out.println(BigDecimal.ONE.negate());
    }
}
