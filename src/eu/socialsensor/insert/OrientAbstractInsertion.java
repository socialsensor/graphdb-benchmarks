/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *  
 */

package eu.socialsensor.insert;

import com.orientechnologies.orient.core.index.OIndex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.apache.log4j.Logger;

import java.util.Arrays;

/**
 * Implementation of single Insertion in OrientDB graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 * 
 */
public abstract class OrientAbstractInsertion implements Insertion {

  public static String      INSERTION_TIMES_OUTPUT_PATH = null;

  protected static int      count;
  protected OIndex          vertices                    = null;
  protected OrientBaseGraph orientGraph                 = null;
  protected Logger          logger                      = Logger.getLogger(OrientAbstractInsertion.class);

  public OrientAbstractInsertion(OrientBaseGraph orientGraph, OIndex vertices) {
    this.orientGraph = orientGraph;
    this.vertices = vertices;
  }

  protected Iterable<OrientVertex> vertexIndexLookup(final int iValue) {
    Object indexValue = vertices.get(iValue);
    if (indexValue != null && !(indexValue instanceof Iterable<?>))
      indexValue = Arrays.asList(indexValue);

    return new OrientElementIterable<OrientVertex>(orientGraph, (Iterable<?>) indexValue);
  }
}
