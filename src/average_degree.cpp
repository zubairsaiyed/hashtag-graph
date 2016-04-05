#include "rapidjson/document.h"
#include <iostream>
#include <fstream>
#include <string>
#include <stdio.h>
#include <time.h>
#include <utility>
#include <queue>
#include <vector>
#include <unordered_map>
#include <regex>
#include <algorithm>
#include <iomanip>

using namespace std;
using namespace rapidjson;


// Edge Abstraction
typedef pair<string, string> Edge;
Edge makeEdge(string v1, string v2) {       // Edge 'constructor' (always an ordered pair to halve the # of edges to track)
    return (v1 < v2) ? make_pair(v1, v2) : make_pair(v2, v1);
}
struct EdgeHash {       // simple hash to enable use as unordered_map key
    size_t operator()(const Edge& e) const {
        hash<string> hashFn;
        return hashFn(e.first + " " + e.second);
    }
};
ostream& operator<< (ostream &out, const Edge &edge) {         // override ostream operator for debugging
    out << edge.first << " <-> " << edge.second;
    return out;
}


// Tweet Abstraction
class Tweet {
    time_t ts;                 // Unix timestamp from tweet
    vector<string> hashTags;        // hashTags in tweet

    public:
        Tweet(const char* json) {               // extract hashtags and timestamp to construct tweet, otherwise throw exception
            hashTags.clear();
            Document d;
            if (d.Parse(json).HasParseError()) {
                throw "Invalid JSON data";
            }
            Value::ConstMemberIterator itr = d.FindMember("timestamp_ms");      // parse JSON using iterators (for optimum memory & speed)
            if (itr != d.MemberEnd() && itr->value.IsString()) {
                ts = (time_t)stoul(itr->value.GetString());
                Value::ConstMemberIterator itr2;
                itr = d.FindMember("entities");
                if ((itr != d.MemberEnd()) && ((itr2 = itr->value.FindMember("hashtags")) != itr->value.MemberEnd()) && itr2->value.IsArray()) {
                    Value::ConstMemberIterator itr4;
                    for (Value::ConstValueIterator itr3 = itr2->value.Begin(); itr3 != itr2->value.End(); ++itr3) {
                        itr4 = itr3->FindMember("text");
                        if (itr4 != itr3->MemberEnd() && itr4->value.IsString())
                            hashTags.push_back(itr4->value.GetString());
                    }
                    dedupeHashTags();       // remove duplicate hashtags
                }
            } else if (d.FindMember("limit") != d.MemberEnd()) {
                throw "Rate limiting message!";
            }
        }

        time_t getTime() const {            // return timestamp of tweet
            return ts;
        }

        int getHashTagCount() const {               // return number of (unique) HashTags in tweet
            return hashTags.size();
        }

        vector<Edge> getEdges() const {                 // generate permutations of hashtags as Edge types
            vector<Edge> edges;
            if (hashTags.size() < 2) return edges;
            for (auto it = hashTags.begin(); it != hashTags.end()-1; ++it) {
                for (auto jt = it+1; jt != hashTags.end(); ++jt) {
                    Edge e = makeEdge(*it,*jt);
                    edges.push_back(makeEdge(*it, *jt));
                }
            }
            return edges;
        }

        friend ostream& operator<< (ostream &out, const Tweet &twt) {       // override ostream operator for debugging
            if (twt.hashTags.size() == 0) {
                out << "[]";
            } else {
                out << "[";
                for (auto it = twt.hashTags.begin(); it!=twt.hashTags.end()-1; ++it)
                    out << *it << ", ";
                out << twt.hashTags.back() << "]";
            }
            return out;
        }

        bool operator<(Tweet other) const {         // override 'less than' operator so priority_queue<Tweet> is effectively a min-heap
            return ts > other.getTime();
        }

    private:
        void dedupeHashTags() {         // remove duplicate hashTags
            auto it = unique(hashTags.begin(), hashTags.end());
            hashTags.resize(distance(hashTags.begin(),it));
        }
};


// HashTag Graph Abstraction
class EdgeGraph {
    unordered_map<Edge, time_t, EdgeHash> edgeMap;      // track (unique) graph edges with most recent timestamp
    unordered_map<string, int> degree;                  // track the degree of each graph vertex
    int edgeCount, vertexCount;                         // counters for efficient calculate of avg degree

    public:
        EdgeGraph() {
            edgeMap.clear();
            degree.clear();
            edgeCount = 0;
            vertexCount = 0;
        }

        void insert(const Tweet& twt) {                 // use Tweet to add corresponding edges and verticies to graph
            vector<Edge> edges = twt.getEdges();
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                if (edgeMap.count(*it)==0) {
                    edgeMap.insert(make_pair(*it, twt.getTime()));
                    edgeCount++;
                } else if (edgeMap.at(*it) < twt.getTime()) {       // if edge already exists and has been tweeted more recently, update timestamp
                    edgeMap.at(*it) = twt.getTime();
                }
                if(degree.count(it->first)==0) {                // track vertex (hashtag) degrees
                    degree.insert(make_pair(it->first, 1));
                    vertexCount++;
                } else {
                    degree.at(it->first)++;
                }
                if(degree.count(it->second)==0) {
                    degree.insert(make_pair(it->second, 1));
                    vertexCount++;
                } else {
                    degree.at(it->second)++;
                }
            }
        }

        void remove(const Tweet& twt) {                     // use Tweet to evict corresponding edges and verticies from graph
            vector<Edge> edges = twt.getEdges();
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                if (edgeMap.count(*it)>0 && edgeMap.at(*it)==twt.getTime()) {       // if edge has not been updated by newer tweet, evict it
                    edgeMap.erase(*it);
                    edgeCount--;
                }
                if (degree.count(it->first)>0 && degree.at(it->first)==1) {        // update vertex (hashtag) degrees
                    degree.erase(it->first);
                    vertexCount--;
                } else {
                    degree.at(it->first)--;
                }
                if (degree.count(it->second)>0 && degree.at(it->second)==1) {
                    degree.erase(it->second);
                    vertexCount--;
                } else {
                    degree.at(it->second)--;
                }
            }
        }

        float getAvgDegree() {              // calculate avg degree of hashtag graph
            return (vertexCount > 0) ? edgeCount*2.0/vertexCount : 0;       // edgecount must by doubled to account for 'reverse edge pairs'
        }

        friend ostream& operator<< (ostream &out, const EdgeGraph &g) {         // override ostream operator for debugging
            for (auto it = g.edgeMap.begin(); it != g.edgeMap.end(); ++it)
                out << it->first << endl;
            out << endl;
            return out;
        }
};


// Twitter HashTag Graph Tracking Algorithm
int main(int argc, char * argv[]) {
    if (argc != 3) {
        cerr << "Error: Run program using the following format 'average_degree' <input file path> <output file path>" << endl;
        return -1;
    }

    ifstream input(argv[1]);
    ofstream output(argv[2]);
    output << fixed << setprecision(2);

    if(input.is_open()) {
        string line;
        time_t currTime = 0;                    // track current (latest) tweet
        priority_queue<Tweet> pq;               // track 'live' tweets in min-heap (for easy eviction of old tweet)
        EdgeGraph g;

        while(getline(input, line)) {           // read json messages from file, line by line
            try {
                Tweet twt(line.c_str());
                if ((twt.getTime() + 60000) <= currTime) {      // if tweet is too old, ignore and continue
                    continue;
                } else if (currTime < twt.getTime()) {          // if tweet is newer than current, evict tweets any obsolete
                    currTime = twt.getTime();
                    while ((pq.size() > 0) && ((pq.top().getTime() + 60000) <= currTime)) {
                        g.remove(pq.top());     // remove tweet from graph abstraction
                        pq.pop();               // stop tracking newly 'dead' tweets
                    }
                }
                pq.push(twt);               // if tweet is 'active' add to min-heap
                g.insert(twt);
                output << trunc(100*g.getAvgDegree())/100 << endl;          // output average degree after processing each tweet
            } catch(const char* msg) {
                //cerr << msg << endl;
                //cerr << "Error causing input: " << endl << line << endl;
                continue;
            }
        }
        input.close();
    } else {
        cerr << "Unable to open file! Check I/O file paths parameters" << endl;
    }

    return 0;
}
