package com.rootscity.places.standardize;


import java.util.Collection;
import java.util.List;

/**
 * User: dallan
 * Date: 1/12/12
 * 
 * Used to analyze the types of errors/warnings seen when standardizing places
 */
public interface ErrorHandler {
   public void tokenNotFound(String text, List<List<String>> levels, int levelNumber, Collection<Integer> matchedParentIds);
   public void skippingParentLevel(String text, List<List<String>> levels, int levelNumber, Collection<Integer> matchedPlaceIds);
   public void typeNotFound(String text, List<List<String>> levels, int levelNumber, Collection<Integer> matchedPlaceIds);
   public void ambiguous(String text, List<List<String>> levels, Collection<Integer> matchedPlaceIds, Place topPlace);
   public void placeNotFound(String text, List<List<String>> levels);
}
