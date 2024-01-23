package org.apache.spark.executor;

import oshi.hardware.*;
import oshi.software.os.FileSystem;
import oshi.software.os.NetworkParams;
import oshi.software.os.OSFileStore;
import oshi.software.os.OperatingSystem;
import oshi.util.FormatUtil;
import oshi.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class SystemInfoTool {
    // on the same machine's JVM, the static leaves no embarrassing scenarios
    static List<String> oshi = new ArrayList<>();
    protected static enum SystemInfoType{
        CPUINFO,
        MEMINFO,
        DISKIOINFO,
        NETWORKINFO,
        DISKUSAGEINFO
    };
    static protected String printSelInfo(SystemInfoType type){
        // flush stale output cache
        oshi.clear();
        // refresh the SystemInfo currently
        // GC relies on automated schematics
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        OperatingSystem os = si.getOperatingSystem();

        switch(type){
            case CPUINFO:
                CentralProcessor processor = hal.getProcessor();
                printCpu(processor);
                break;
            case MEMINFO:
                GlobalMemory memory = hal.getMemory();
                printMemory(memory);
                break;
            case DISKUSAGEINFO:
                printFileSystem(os.getFileSystem());
                break;
            case NETWORKINFO:
                printNetworkInterfaces(hal.getNetworkIFs());
                printNetworkParameters(os.getNetworkParams());
                break;
            default:
                break;
        }

        StringBuilder output = new StringBuilder();
        for (String line : oshi) {
            output.append(line);
            if (line != null && !line.endsWith("\n")) {
                output.append('\n');
            }
        }
        return output.toString();
    }

    protected static void printCpu(CentralProcessor processor) {
        long[] prevTicks = processor.getSystemCpuLoadTicks();
        // Wait a second...
        // must wait, or the CPU occupation would be wrong
        Util.sleep(1000);
        // long[][] prevProcTicks = processor.getProcessorCpuLoadTicks();
        oshi.add(String.format(Locale.ROOT, "CPU load [overall]: %.1f%%",
                processor.getSystemCpuLoadBetweenTicks(prevTicks) * 100));
        // NOTE: backup for pending ideas
        // per core CPU
        // StringBuilder procCpu = new StringBuilder("CPU load per processor:");
        // double[] load = processor.getProcessorCpuLoadBetweenTicks(prevProcTicks);
        // for (double avg : load) {
        //     procCpu.append(String.format(Locale.ROOT, " %.1f%%", avg * 100));
        // }
        // oshi.add(procCpu.toString());
        // long[] freqs = processor.getCurrentFreq();
        // if (freqs[0] > 0) {
        //     StringBuilder sb = new StringBuilder("Current Frequencies: ");
        //     for (int i = 0; i < freqs.length; i++) {
        //         if (i > 0) {
        //             sb.append(", ");
        //         }
        //         sb.append(FormatUtil.formatHertz(freqs[i]));
        //     }
        //     oshi.add(sb.toString());
        // }
    }

    protected static double getGeneralResourceUsage() {
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        OperatingSystem os = si.getOperatingSystem();
        CentralProcessor processor = hal.getProcessor();
        long[] prevTicks = processor.getSystemCpuLoadTicks();
        Util.sleep(1000);
        double cpuUsage = processor.getSystemCpuLoadBetweenTicks(prevTicks);
        GlobalMemory memory = hal.getMemory();
        double memoryUsage = (memory.getTotal() - memory.getAvailable()) / (double)memory.getTotal();
        System.out.println("wwwwwwww cpu: " + cpuUsage);
        System.out.println("wwwwwwww memory: " + memoryUsage);
        double U = 0.6 * cpuUsage + 0.4 * memoryUsage;
        return U;
    }

    protected static void printMemory(GlobalMemory memory) {
        // could directly print out the physical memory usage
        // virtual memory usage not taking into consideration for now
        oshi.add("Physical Memory [primary]: " + memory.toString());
        // NOTE: seems some side-way physical memory information may be obtained if possible
        List<PhysicalMemory> pmList = memory.getPhysicalMemory();
        if (!pmList.isEmpty()) {
            oshi.add("Physical Memory [secondary]: ");
            for (PhysicalMemory pm : pmList) {
                oshi.add(" " + pm.toString());
            }
        }
    }

    private static void printFileSystem(FileSystem fileSystem) {
        oshi.add("File System:");

        for (OSFileStore fs : fileSystem.getFileStores()) {
            if(fs.getName()=="/"){
                long usable = fs.getUsableSpace();
                long total = fs.getTotalSpace();
                oshi.add(String.format(Locale.ROOT,
                        " %s (%s) [%s] %s of %s free (%.1f%%), %s of %s files free (%.1f%%) is %s "
                                + (fs.getLogicalVolume() != null && fs.getLogicalVolume().length() > 0 ? "[%s]" : "%s")
                                + " and is mounted at %s",
                        fs.getName(), fs.getDescription().isEmpty() ? "file system" : fs.getDescription(), fs.getType(),
                        FormatUtil.formatBytes(usable), FormatUtil.formatBytes(fs.getTotalSpace()), 100d * usable / total,
                        FormatUtil.formatValue(fs.getFreeInodes(), ""), FormatUtil.formatValue(fs.getTotalInodes(), ""),
                        100d * fs.getFreeInodes() / fs.getTotalInodes(), fs.getVolume(), fs.getLogicalVolume(),
                        fs.getMount()));
            }
        }
    }

    private static void printNetworkInterfaces(List<NetworkIF> list) {
        StringBuilder sb = new StringBuilder("Network Interfaces:");
        if (list.isEmpty()) {
            sb.append(" Unknown");
        } else {
            for (NetworkIF net : list) {
                net.updateAttributes();
                sb.append("\n ").append(net.toString());
                sb.append("\n").append(String.format("The Speed of the network interface is: %d",net.getSpeed()));
            }
        }
        oshi.add(sb.toString());
    }

    private static void printNetworkParameters(NetworkParams networkParams) {
        oshi.add("Network parameters:\n " + networkParams.toString());
    }

}
