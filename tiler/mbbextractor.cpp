#include "hadoopgis.h"
#include "cmdline.h"

/* The program extracts the minimum bounding boxes of objects */

/* Extract the MBBs of spatial objects. 
 * Commandline arguments: 
 *         argv1 is the field number of the object ID, 
 *         argv2 is the field number of geometry 
 *         (counting from 1)
 * Output format:
 *         min_x TAB min_y TAB max_x TAB max_y
 * */

GeometryFactory *gf = NULL;
WKTReader *wkt_reader = NULL;
IStorageManager * storage = NULL;
ISpatialIndex * spidx = NULL;
map<id_type,string> id_polygon ;
vector<id_type> hits ; 
char * prefix;

int GEOM_IDX = -1;
int ID_IDX = -1;

bool assignIndex(int uid_idx, int geom_idx) {
  /* if (uid_idx <1 || geom_idx <1 ) {
     *
    cout << "Got here";
	    return false;
    } */
    GEOM_IDX = geom_idx -1;
    ID_IDX = uid_idx -1;
    return true;
}

void freeObjects() {
    // garbage collection 
    delete wkt_reader ;
    delete gf ; 
    delete spidx;
    delete storage;
}

vector<string> parse(string & line) {
  vector<string> tokens ;
  tokenize(line, tokens,TAB,true);
  return tokens;
}

int main(int argc, char **argv) {
  double min_x;
  double max_x;
  double min_y;
  double max_y;

  if (! assignIndex(atoi(argv[1]), atoi(argv[2])) ) {
    cerr << "Invalid arguments for field indices" << endl;
    return -1;
  }


  // initlize the GEOS ibjects
  gf = new GeometryFactory(new PrecisionModel(),0);
  wkt_reader= new WKTReader(gf);


  // process input data 
  map<int,Geometry*> geom_polygons;
  string input_line;
  vector<string> fields;
  cerr << "Reading input from stdin..." <<endl; 
  id_type id ; 
  Geometry* geom; 
  const Envelope * env;


  while(cin && getline(cin, input_line) && !cin.eof()){
    fields = parse(input_line);
    if (fields[ID_IDX].length() <1 )
      continue ;  // skip lines which has empty id field 
    id = std::strtoul(fields[ID_IDX].c_str(), NULL, 0);

    if (fields[GEOM_IDX].length() <2 )
    {
#ifndef NDEBUG
      cerr << "skipping record [" << id <<"]"<< endl;
#endif
      continue ;  // skip lines which has empty geometry
    }
    // try {
    geom = wkt_reader->read(fields[GEOM_IDX]);
    env = geom->getEnvelopeInternal();
    cout << fields[ID_IDX] << TAB  << env->getMinX() << TAB << env->getMinY() << TAB 
        << env->getMaxX() << TAB << env->getMaxY() << endl;
  }

  cout.flush();
  cerr.flush();
  freeObjects();
  return 0; // success
}

