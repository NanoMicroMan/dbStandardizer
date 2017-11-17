package com.rootscity.places;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.rootscity.common.MultiSTD;
import com.rootscity.common.stats;
import com.rootscity.places.standardize.Place;
import com.rootscity.places.standardize.Standardizer;

import java.io.InputStreamReader;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class testPlaces {
	public static transient Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	public TreeMap<String, String> placeMap = new TreeMap<>();

	public static void main(String[] args) {
		testPlaces tp  = new testPlaces();
		tp.run();
	}

	public void run() {
		InputStreamReader bis = new InputStreamReader(ClassLoader.getSystemResourceAsStream("PlacesMap.json"));
		JsonReader jr = new JsonReader(bis);
		placeMap = gson.fromJson(jr, placeMap.getClass());
		int eq = 0;
		int diff = 0;
		Stopwatch sw = Stopwatch.createStarted();
		Standardizer.getInstance();
		sw.stop();
		Long ms = sw.elapsed(TimeUnit.MILLISECONDS);
		System.out.println("\nInit: " + ms/1000.0);
		sw.reset();
		sw.start();
		for (String p: placeMap.keySet()) {
			Place result = Standardizer.getInstance().standardize(p);
			String res = result==null? "": result.getFullName();
			String oldRes = placeMap.get(p);
			if (res.equalsIgnoreCase(oldRes)) {
				eq++;
			}
			else {
				diff++;
				System.out.println(p + " |\t" + res + " |\t" + oldRes);
			}
		}
		sw.stop();
		ms = sw.elapsed(TimeUnit.MILLISECONDS);
		System.out.println("\n Identical: " + eq + " \tDiff: " + diff + "\tElapsed: " + ms/1000.0);
	}

	public void runOld() {
		InputStreamReader bis = new InputStreamReader(ClassLoader.getSystemResourceAsStream("PlacesMap.json"));
		JsonReader jr = new JsonReader(bis);
		placeMap = gson.fromJson(jr, placeMap.getClass());
		Stopwatch sw = Stopwatch.createStarted();
		MultiSTD<String> std = new MultiSTD<String>(null, "http://localhost:2016", 5, true) {
			@Override
			public void batchReady(BatchContainer bc) {
			}
		};
		for (String p: placeMap.keySet()) {
			String oldRes = placeMap.get(p);
			std.addPlace(oldRes, p);
		}
		std.start();
		std.waitQueue(new stats("test", 0L, 10000L));
		std.close(false);
		sw.stop();
		Long ms = sw.elapsed(TimeUnit.MILLISECONDS);
		System.out.println("\n\nElapsed: " + ms/1000.0);
	}

}
