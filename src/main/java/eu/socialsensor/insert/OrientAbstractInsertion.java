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

import org.apache.log4j.Logger;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientExtendedGraph;

/**
 * Implementation of single Insertion in OrientDB graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 * 
 */
public abstract class OrientAbstractInsertion implements Insertion {
	
	public static String INSERTION_TIMES_OUTPUT_PATH = null;

	protected OrientExtendedGraph orientGraph = null;
	protected Logger logger = Logger.getLogger(OrientAbstractInsertion.class);

	protected OIndex<?> index;

	public OrientAbstractInsertion(OrientExtendedGraph orientGraph) {
		this.orientGraph = orientGraph;
	}

	protected Vertex getOrCreate(final String value) {
		final int key = Integer.parseInt(value);

		Vertex v;
		if (index == null) {
			index = orientGraph.getRawGraph().getMetadata().getIndexManager().getIndex("V.nodeId");
		}
			
		final OIdentifiable rec = (OIdentifiable) index.get(key);
		if (rec != null) {
			return orientGraph.getVertex(rec);
		}
		
		v = orientGraph.addVertex(key, "nodeId", key);
  
		if (orientGraph instanceof TransactionalGraph) {
			((TransactionalGraph) orientGraph).commit();
		}

		return v;
	}
}
