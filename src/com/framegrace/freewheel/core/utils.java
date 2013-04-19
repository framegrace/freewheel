/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.framegrace.freewheel.core;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.RandomBasedGenerator;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import java.math.BigInteger;
import java.util.UUID;

/**
 *
 * @author marc
 */
public class utils {

    private static final ThreadLocal< RandomBasedGenerator> uniqueNum =
            new ThreadLocal< RandomBasedGenerator>() {
        @Override
        protected RandomBasedGenerator initialValue() {
            return Generators.randomBasedGenerator();
        }
    };

    static long getUUID() {
        UUID uid = null;
        do {
            uid = uniqueNum.get().generate();
        } while (uid.getMostSignificantBits() == 0);
        return uid.getMostSignificantBits();
    }
}
