package edu.cmu.scs.cc.project1;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.junit.jupiter.api.Test;


/**
 * Usage:
 * mvn test
 *
 * <p>You should pass all the provided test cases before you make any submission.
 *
 * <p>Feel free to add more test cases.
 */
class DataFilterTest {

    @Test
    void getColumns() {
        assertTrue(Arrays.equals(
                DataFilter.getColumns("en Carnegie_Mellon_University 34 0"),
                new String[] {"en", "Carnegie_Mellon_University", "34", "0"}));
        assertTrue(Arrays.equals(
                DataFilter.getColumns("en User%3AK6ka 34 0"),
                new String[] {"en", "User:K6ka", "34", "0"}));
    }

    @Test
    void checkDataLength() {
        assertTrue(DataFilter.checkDataLength(
                DataFilter.getColumns("en Carnegie_Mellon_University 34 0")));
        assertFalse(DataFilter.checkDataLength(
                DataFilter.getColumns("en 34 0")));
        assertFalse(DataFilter.checkDataLength(
                DataFilter.getColumns("en Carnegie_Mellon_University 34 34 0")));
        assertFalse(DataFilter.checkDataLength(
                DataFilter.getColumns("en Carnegie_Mellon_University%2034 34 0")));
    }

    @Test
    void checkDomain() {
        assertTrue(DataFilter.checkDomain(
                DataFilter.getColumns("en Carnegie_Mellon_University 34 0")));
        assertTrue(DataFilter.checkDomain(
                DataFilter.getColumns("en.m Carnegie_Mellon_University 34 0")));
        assertFalse(DataFilter.checkDomain(
                DataFilter.getColumns("fr Carnegie_Mellon_University 34 0")));
    }

    @Test
    void checkSpecialPage() {
        assertTrue(DataFilter.checkSpecialPage(
                DataFilter.getColumns("en Carnegie_Mellon_University 34 0")));
        assertFalse(DataFilter.checkSpecialPage(
                DataFilter.getColumns("en Main_Page 34 0")));
        assertFalse(DataFilter.checkSpecialPage(
                DataFilter.getColumns("en - 34 0")));
        assertFalse(DataFilter.checkSpecialPage(
                DataFilter.getColumns("en %2D 34 0")));
    }

    @Test
    void checkPrefix() {
        assertTrue(DataFilter.checkPrefix(
                DataFilter.getColumns("en Carnegie_Mellon_University 34 0")));
        assertFalse(DataFilter.checkPrefix(
                DataFilter.getColumns("en User:K6ka 34 0")));
        assertFalse(DataFilter.checkPrefix(
                DataFilter.getColumns("en User%3AK6ka 34 0")));
        assertFalse(DataFilter.checkPrefix(
                DataFilter.getColumns("en User%3aK6ka 34 0")));
    }

    @Test
    void checkSuffix() {
        assertTrue(DataFilter.checkSuffix(
            DataFilter.getColumns("en Carnegie_Mellon_University 34 0")));
        assertFalse(DataFilter.checkSuffix(
            DataFilter.getColumns("en Carnegie_Mellon_University.jpg 34 0")));
        assertFalse(DataFilter.checkSuffix(
            DataFilter.getColumns("en cat.PNG 34 0")));
        assertFalse(DataFilter.checkSuffix(
            DataFilter.getColumns("en cat%2EPNG 34 0")));
    }

    @Test
    void checkFirstLetter() {
        assertTrue(DataFilter.checkFirstLetter(
            DataFilter.getColumns("en Carnegie_Mellon_University 34 0")));
        assertFalse(DataFilter.checkFirstLetter(
            DataFilter.getColumns("en carnegie_Mellon_University 34 0")));
        assertTrue(DataFilter.checkFirstLetter(
            DataFilter.getColumns("en Î¹Ï€Ï…Ï 254860 0")));
        assertTrue(DataFilter.checkFirstLetter(
            DataFilter.getColumns("en 1a 34 0")));
    }

    @Test
    void checkAllRules() {
        assertTrue(DataFilter.checkAllRules(
            DataFilter.getColumns("en Carnegie_Mellon_University 34 0")));
        assertFalse(DataFilter.checkAllRules(
            DataFilter.getColumns("en carnegie_Mellon_University%2034 34 0")));
        assertFalse(DataFilter.checkAllRules(
            DataFilter.getColumns("fr Main_Page 34 0")));
        assertFalse(DataFilter.checkAllRules(
            DataFilter.getColumns("en special:cmu 34 0")));
    }
}