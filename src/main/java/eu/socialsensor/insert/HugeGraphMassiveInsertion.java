/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package eu.socialsensor.insert;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;

import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.HugeGraphUtils;

/**
 * Created by zhangsuochao on 17/6/21.
 */
public class HugeGraphMassiveInsertion extends InsertionBase<Vertex> {

    private static final Logger LOG = LogManager.getLogger();
    private final GraphManager graphManager;
    private Map<String,Vertex> cache = new HashMap<>();

    private List<Vertex> vertexList = new LinkedList<>();
    private List<Edge> edgeList = new LinkedList<>();
    private transient int vertexCounter = 0; //
    private transient int edgeCounter = 0;
    public HugeGraphMassiveInsertion(GraphManager graphManager) {
        super(GraphDatabaseType.HUGEGRAPH_CASSANDRA, null /* resultsPath */);
        this.graphManager = graphManager;
    }

    @Override
    protected Vertex getOrCreate(String value) {
        Vertex vertex = null;
        if(!HugeGraphUtils.isStringEmpty(value)){
            String id = HugeGraphUtils.createId("node",value);
            vertex = cache.get(id);
            if (vertex == null){
//                vertex = graphManager.addVertex(T.label,"node","nodeId",value);
                vertex = new Vertex("node").property("nodeId",value);
                vertexList.add(vertex);
//
//                if(++vertexCounter%100==0){
//                    graphManager.addVertices(vertexList);
//                    vertexList.clear();
//                }
                cache.put(id,vertex);
            }

        }

        return vertex;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest) {


        Edge edge = new Edge("link");
        if(HugeGraphUtils.isStringEmpty(src.id())){
            String srcId = HugeGraphUtils.createId("node",(String)src.properties().get("nodeId"));
            edge.source(srcId);
            edge.sourceLabel("node");
        }else{
            edge.source(src);
        }

        if(HugeGraphUtils.isStringEmpty(dest.id())){
            String destId = HugeGraphUtils.createId("node",(String)dest.properties().get("nodeId"));
            edge.target(destId);
            edge.targetLabel("node");
        }else {
            edge.target(dest);
        }

        edgeList.add(edge);

        if(++edgeCounter%100==0){
            graphManager.addVertices(vertexList);
            vertexList.clear();
//            graphManager.addEdge(src,"link",dest);
            graphManager.addEdges(edgeList);
            edgeList.clear();
        }
    }

    @Override
    protected void post(){
        if(vertexList.size()>0){
            graphManager.addVertices(vertexList);
            LOG.info("Post {} vertices",vertexList.size());
            vertexList.clear();
        }

        if (edgeList.size()>0){
            graphManager.addEdges(edgeList);
            LOG.info("Post {} edges",edgeList.size());
            edgeList.clear();

        }
    }

}
