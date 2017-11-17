/*
 * Copyright 2012 Foundation for On-Line Genealogy, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rootscity.places.standardize;

import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.StringJoiner;

/**
 * User: dallan
 * Date: 1/10/12
 */
@XmlRootElement
public class Place implements Serializable, Comparable {
	protected static final transient long serialVersionUID = 3217229562337614774L;

   @Override
   public int compareTo(@NotNull Object o) {
      return Integer.compare (id, ((Place)o).id);
   }

   public static class AltName implements Serializable {
	   protected static final transient long serialVersionUID = 1722191562337614774L;
      public String altName;
      public String source;
      public AltName(String altName, String source) {
         this.altName = altName;
         this.source = source;
      }
      public AltName() {
         this(null, null);
      }
   }

   public static class Source implements Serializable {
	   protected static final transient long serialVersionUID = 1722935623337614774L;
      public String source;
      public String id;
      public Source(String source, String id) {
         this.source = source;
         this.id = id;
      }
      public Source() {
         this(null, null);
      }
   }

	private int id = 0;
   private String name = null;
   private AltName[] altNames = new AltName[0];
   private String[] types = new String[0];
   private int locatedInId = 0;
   private int[] alsoLocatedInIds = new int[0];
   private int level = 0;
   private int countryId = 0;
   private double latitude = 0.0;
   private double longitude = 0.0;
   private Source[] sources = new Source[0];

   public void addAltName(String altName, String src) {
      ArrayUtils.add(altNames, new AltName(altName, src));
   }

   public int getId() {
      return id;
   }

   public void setId(int id) {
      this.id = id;
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public AltName[] getAltNames() {
      return altNames;
   }

   public void setAltNames(AltName[] altNames) {
      this.altNames = altNames == null ? new AltName[0] : altNames;
   }

   public String[] getTypes() {
      return types;
   }

   public void setTypes(String[] types) {
      this.types = types == null ? new String[0] : types;
   }

   public int getLocatedInId() {
      return locatedInId;
   }

   public void setLocatedInId(int locatedInId) {
      this.locatedInId = locatedInId;
   }

   public int[] getAlsoLocatedInIds() {
      return alsoLocatedInIds;
   }

   public void setAlsoLocatedInIds(int[] alsoLocatedInIds) {
      this.alsoLocatedInIds = alsoLocatedInIds == null ? new int[0] : alsoLocatedInIds;
   }

   public int getLevel() {
      return level;
   }

   public void setLevel(int level) {
      this.level = level;
   }

   public int getCountryId() {
      return countryId;
   }

   public void setCountryId(int countryId) {
      this.countryId = countryId;
   }

   public double getLatitude() {
      return latitude;
   }

   public void setLatitude(double latitude) {
      this.latitude = latitude;
   }

   public double getLongitude() {
      return longitude;
   }

   public void setLongitude(double longitude) {
      this.longitude = longitude;
   }

   public Source[] getSources() {
      return sources;
   }

   public void setSources(Source[] sources) {
      this.sources = sources == null ? new Source[0] : sources;
   }

   @XmlElement
   public String getFullName() {
      StringJoiner buf = new StringJoiner(", ");
      buf.add(getName());
      int locatedIn = getLocatedInId();
	   int levels = 0;
	   while (locatedIn > 0 && ++levels < 10) {
		   Place p = Standardizer.getInstance().getPlace(locatedIn);
         buf.add(p.getName());
         locatedIn = p.getLocatedInId();
      }
      if (levels >= 10) {
         return "";
      }
      return buf.toString();
   }
}
