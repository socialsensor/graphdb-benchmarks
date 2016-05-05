package eu.socialsensor.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import eu.socialsensor.graphdatabases.GraphDatabase;

/**
 * Implementation of Louvain Method on top of graph databases. Gephi Toolkit
 * (https://gephi.org/toolkit/) java implementation was used as guide.
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class LouvainMethod
{
    private final Random random;
    private double resolution = 1.0;
    private double graphWeightSum;
    private int nodeCount;
    private List<Double> communityWeights;
    private boolean communityUpdate = false;

    GraphDatabase<?,?,?,?> graphDatabase;
    Cache cache;

    public LouvainMethod(GraphDatabase<?,?,?,?> graphDatabase, int cachePercentage, Random random) throws ExecutionException
    {
        this.graphDatabase = graphDatabase;
        this.random = random;
        initialize();
        cache = new Cache(graphDatabase, cachePercentage, nodeCount);
    }

    private void initialize()
    {
        this.nodeCount = this.graphDatabase.getNodeCount();// this step takes a long
                                                   // time on dynamodb.
        this.graphWeightSum = this.graphDatabase.getGraphWeightSum() / 2;

        this.communityWeights = new ArrayList<Double>(this.nodeCount);
        for (int i = 0; i < this.nodeCount; i++)
        {
            this.communityWeights.add(0.0);
        }

        this.graphDatabase.initCommunityProperty();
    }

    public void computeModularity() throws ExecutionException
    {
        boolean someChange = true;
        while (someChange)
        {
            someChange = false;
            boolean localChange = true;
            while (localChange)
            {
                localChange = false;
                int start = 0;
                if (null != this.random)
                {
                    start = Math.abs(random.nextInt()) % this.nodeCount;
                }
                int step = 0;
                for (int i = start; step < this.nodeCount; i = (i + 1) % this.nodeCount)
                {
                    step++;
                    int bestCommunity = updateBestCommunity(i);
                    if ((this.cache.getCommunity(i) != bestCommunity) && (this.communityUpdate))
                    {

                        this.cache.moveNodeCommunity(i, bestCommunity);
                        this.graphDatabase.moveNode(i, bestCommunity);

                        double bestCommunityWeight = this.communityWeights.get(bestCommunity);

                        bestCommunityWeight += cache.getNodeCommunityWeight(i);
                        this.communityWeights.set(bestCommunity, bestCommunityWeight);
                        localChange = true;
                    }

                    this.communityUpdate = false;
                }
                someChange = localChange || someChange;
            }
            if (someChange)
            {
                zoomOut();
            }
        }
    }

    private int updateBestCommunity(int node) throws ExecutionException
    {
        int bestCommunity = 0;
        double best = 0;
        Set<Integer> communities = this.cache.getCommunitiesConnectedToNodeCommunities(node);
        for (int community : communities)
        {
            double qValue = q(node, community);
            if (qValue > best)
            {
                best = qValue;
                bestCommunity = community;
                this.communityUpdate = true;
            }
        }
        return bestCommunity;
    }

    private double q(int nodeCommunity, int community) throws ExecutionException
    {
        double edgesInCommunity = this.cache.getEdgesInsideCommunity(nodeCommunity, community);
        double communityWeight = this.communityWeights.get(community);
        double nodeWeight = this.cache.getNodeCommunityWeight(nodeCommunity);
        double qValue = this.resolution * edgesInCommunity - (nodeWeight * communityWeight)
            / (2.0 * this.graphWeightSum);
        int actualNodeCom = this.cache.getCommunity(nodeCommunity);
        int communitySize = this.cache.getCommunitySize(community);

        if ((actualNodeCom == community) && (communitySize > 1))
        {
            qValue = this.resolution * edgesInCommunity - (nodeWeight * (communityWeight - nodeWeight))
                / (2.0 * this.graphWeightSum);
        }
        if ((actualNodeCom == community) && (communitySize == 1))
        {
            qValue = 0.;
        }
        return qValue;
    }

    public void zoomOut()
    {
        this.nodeCount = this.graphDatabase.reInitializeCommunities();
        this.cache.reInitializeCommunities();
        this.communityWeights = new ArrayList<Double>(this.nodeCount);
        for (int i = 0; i < this.nodeCount; i++)
        {
            this.communityWeights.add(graphDatabase.getCommunityWeight(i));
        }
    }

    public int getNodeCount()
    {
        return this.nodeCount;
    }

}