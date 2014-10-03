#include "resquecommon.h"
#include <iostream>

/* The program extracts the minimum bounding boxes of objects */

/* Extract the MBBs of spatial objects. 
 * Commandline arguments: 
 *         argv1 is the field number of the object ID, 
 *         argv2 is the sampling ratio 
 *         (counting from 1)
 * Output format:
 *         some_id min_x TAB min_y TAB max_x TAB max_y
 * */


GeometryFactory *gf = NULL;
WKTReader *wkt_reader = NULL;
IStorageManager * storage = NULL;
ISpatialIndex * spidx = NULL;

int GEOM_IDX = -1;

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

  if (argc < 6) {
	cerr << "Usage: "<< argv[0] << " [min_x] [min_y] [max_x] [max_y]" << endl;
        return -1;
  }

  GEOM_IDX = atoi(argv[5]) + 1 ; // -1 + 2 (partition id + legacy field)
  if (GEOM_IDX < 1) {
    cerr << "Invalid arguments for field indices" << endl;
    return -1;
  }

  // initlize the GEOS objects
  gf = new GeometryFactory(new PrecisionModel(),0);
  wkt_reader= new WKTReader(gf);

  Geometry* window;
  stringstream ss;
  min_x = strtod(argv[1], NULL);
  min_y = strtod(argv[2], NULL);
  max_x = strtod(argv[3], NULL);
  max_y = strtod(argv[4], NULL);

  ss << shapebegin << min_x << SPACE << min_y << COMMA
         << min_x << SPACE << max_y << COMMA
         << max_x << SPACE << max_y << COMMA
         << max_x << SPACE << min_y << COMMA
         << min_x << SPACE << min_y << shapeend;
  window = wkt_reader->read(ss.str());
  // process input data 
  //
  map<int,Geometry*> geom_polygons;
  string input_line;
  vector<string> fields;
  cerr << "Reading input from stdin..." <<endl; 
  id_type id ; 
  Geometry* geom; 
  const Envelope * env;


  while(cin && getline(cin, input_line) && !cin.eof()){
    fields = parse(input_line);
    //if (fields[ID_IDX].length() <1 )
    //  continue ;  // skip lines which has empty id field 
    // id = std::strtoul(fields[ID_IDX].c_str(), NULL, 0);

    if (fields[GEOM_IDX].length() <2 )
    {
#ifndef NDEBUG
      cerr << "skipping record [" << id <<"]"<< endl;
#endif
      continue ;  // skip lines which has empty geometry
    }
    // try {
    geom = wkt_reader->read(fields[GEOM_IDX]);
    if (geom->intersects(window)) {
        cout << input_line << endl;
    }
  }

  cout.flush();
  cerr.flush();
  freeObjects();
  return 0; // success
}

