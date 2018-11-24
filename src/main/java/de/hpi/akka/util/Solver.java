package de.hpi.akka.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/*
* Based on SerialAnalyzer from Material.
* Removed hard coding and did some array to list conversions for easier handling of values in the main application.
*/
public class Solver {

    public static int unHash(String hexHash) {
        for (int i = 0; i < Integer.MAX_VALUE; i++)
            if (hash(i).equals(hexHash))
                return i;
        throw new RuntimeException("Cracking failed for " + hexHash);
    }

    private static String hash(int number) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(number).getBytes("UTF-8"));

            StringBuffer stringBuffer = new StringBuffer();
            for (int i = 0; i < hashedBytes.length; i++) {
                stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        }
        catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public static int sum(int[] numbers, int[] prefixes) {
        int sum = 0;
        for (int i = 0; i < numbers.length; i++)
            sum += numbers[i] * prefixes[i];
        return sum;
    }

    public static int longestOverlapPartner(int thisIndex, List<String> sequences) {
        int bestOtherIndex = -1;
        String bestOverlap = "";
        for (int otherIndex = 0; otherIndex < sequences.size(); otherIndex++) {
            if (otherIndex == thisIndex)
                continue;

            String longestOverlap = longestOverlap(sequences.get(thisIndex), sequences.get(otherIndex));

            if (bestOverlap.length() < longestOverlap.length()) {
                bestOverlap = longestOverlap;
                bestOtherIndex = otherIndex;
            }
        }
        return bestOtherIndex;
    }

    private static String longestOverlap(String str1, String str2) {
        if (str1.isEmpty() || str2.isEmpty())
            return "";

        if (str1.length() > str2.length()) {
            String temp = str1;
            str1 = str2;
            str2 = temp;
        }

        int[] currentRow = new int[str1.length()];
        int[] lastRow = str2.length() > 1 ? new int[str1.length()] : null;
        int longestSubstringLength = 0;
        int longestSubstringStart = 0;

        for (int str2Index = 0; str2Index < str2.length(); str2Index++) {
            char str2Char = str2.charAt(str2Index);
            for (int str1Index = 0; str1Index < str1.length(); str1Index++) {
                int newLength;
                if (str1.charAt(str1Index) == str2Char) {
                    newLength = str1Index == 0 || str2Index == 0 ? 1 : lastRow[str1Index - 1] + 1;

                    if (newLength > longestSubstringLength) {
                        longestSubstringLength = newLength;
                        longestSubstringStart = str1Index - (newLength - 1);
                    }
                } else {
                    newLength = 0;
                }
                currentRow[str1Index] = newLength;
            }
            int[] temp = currentRow;
            currentRow = lastRow;
            lastRow = temp;
        }
        return str1.substring(longestSubstringStart, longestSubstringStart + longestSubstringLength);
    }

    public static String findHash(int content, int prefix) {
        String fullPrefix = (prefix > 0) ? "11111" : "00000";
        Random rand = new Random(13);

        int nonce;
        while (true) {
            nonce = rand.nextInt();
            String hash = hash(content + nonce);
            if (hash.startsWith(fullPrefix))
                return hash;
        }
    }
}