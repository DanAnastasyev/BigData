package ru.mipt.bigdata;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class IpRangeUDTF extends GenericUDTF {
    private StringObjectInspector subnetAddressIO = null;
    private StringObjectInspector maskAddressIO = null;
    private Object[] forwardObjArray = new Object[2];

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentException(getClass().getSimpleName() + " takes only 2 argument!");
        }

        subnetAddressIO = (StringObjectInspector) args[0];
        maskAddressIO = (StringObjectInspector) args[1];

        final List<String> fieldNames = Arrays.asList("IpAddress", "IpAddressValue");
        final List<ObjectInspector> fieldsOIs = Arrays.<ObjectInspector>asList(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaLongObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldsOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        String subnet = subnetAddressIO.getPrimitiveJavaObject(objects[0]);
        String mask = maskAddressIO.getPrimitiveJavaObject(objects[1]);

        int numberOfIpAddresses = getNumberOfPossibleHost(mask);

        int[] parsedIp = parseIpAddress(subnet);
        getNextIpAddress(parsedIp); // Переходим к первому ip (пропуская адрес подсети)

        for (int i = 0; i < numberOfIpAddresses - 2; ++i) {
            forwardObjArray[0] = String.format("%d.%d.%d.%d",
                    parsedIp[0], parsedIp[1], parsedIp[2], parsedIp[3]);
            // Не уверен, что понимаю правильно фразу про численные значения ip-адресов
            forwardObjArray[1] = convertIpToValue(parsedIp);
            forward(forwardObjArray);
            getNextIpAddress(parsedIp);
        }
    }

    private long convertIpToValue(int[] ipAddress) {
        long ipAddr = 0;
        for (int i = 0; i < ipAddress.length; ++i) {
            ipAddr += (long) ipAddress[i] << (8 * (3 - i));
        }
        return ipAddr;
    }

    private int getNumberOfPossibleHost(String mask) {
        long ipAddr = 0;
        String[] vals = mask.split("\\.");
        for (int i = 0; i < vals.length; ++i) {
            ipAddr += Long.parseLong(vals[i]) << (8 * (3 - i));
        }

        BitSet bits = BitSet.valueOf(new long[]{ ipAddr });

        return (int) Math.pow(2, 32 - bits.cardinality());
    }

    private int[] parseIpAddress(String address) {
        int[] ipAddr = new int[4];
        String[] vals = address.split("\\.");
        for (int i = 0; i < vals.length; ++i) {
            ipAddr[i] = Integer.parseInt(vals[i]);
        }
        return ipAddr;
    }

    private void getNextIpAddress(int[] ipAddress) {
        ++ipAddress[3];
        for (int i = 3; i >= 0; --i) {
            if (ipAddress[i] > 255) {
                ipAddress[i] = 0;
                if (i > 0) {
                    ++ipAddress[i - 1];
                }
            }
        }
    }

    @Override
    public void close() throws HiveException {
    }
}
