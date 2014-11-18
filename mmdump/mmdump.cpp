#include <iostream>
#include <chrono>

#include <float.h>
#include <stdint.h>

#include <boost/geometry.hpp>

#include <Poco/Logger.h>
#include <folly/SmallLocks.h>
#include <folly/AtomicHashMap.h>

#include <base_types.hpp>
#include <map_parser.hpp>

using namespace std;
using namespace std::chrono;

struct LogDataEntry {
	log_data_t data;
	folly::MicroSpinLock lock;
	uint8_t padding; //not used for the meantime.
	uint16_t last_bearing; //uint-encoded bearing [0, 2 * PI] -> [0, 65535]

	//probably additional statistical data?

	double get_bearing() const {
		if ( last_bearing == 0 )
			return NAN;

		return double(last_bearing - 1) / double((1 << 16) - 1) * 2.0 * M_PI;
	}

	void set_bearing(double radians) {
		if ( std::isnan(radians) )
			last_bearing = 0;
		else {
			last_bearing = (uint16_t)round(
				fmod(fmod(radians, 2.0 * M_PI) + 2.0 * M_PI, 2.0 * M_PI) / (2.0 * M_PI)
				* ((1 << 16) - 1));
		}
	}
};

static constexpr double dist_factor = 0.0266, heading_factor = 0.0231;


int main(int argc, char **argv) {
	
	if ( argc <= 1 ) {
		cerr << "Usage: " << argv[0] << " mapfile.osm.pbf\n";
		return 1;
	}

	Poco::Logger::setLevel("", Poco::Message::PRIO_FATAL);

	auto mapFile = readMapFile(argv[1]);
	if ( !mapFile ) {
		cerr << "Unable to read mapfile.\n";
		return 1;
	}

	RoadNetwork &rn = mapFile.get();

	folly::AtomicHashMap<uint64_t, LogDataEntry> m_last_log(1000);

	cout << "user_id,time,lon,lat,snapped_lon,snapped_lat,speed\n";
	cout.precision(DBL_DIG);

	uint64_t user_id;

	while ( cin >> user_id ) {
		log_data_t cur;
		cur.user_id = user_id;

	
		uint64_t centiseconds;
		double _lon, _lat;
		cin >> centiseconds >> _lon >> _lat;
		cur.pos.lon(_lon);
		cur.pos.lat(_lat);
		cur.timestamp = timestamp_t(duration_cast<timestamp_t::duration>(centiseconds_t(centiseconds)));
		

		auto ret = m_last_log.insert(cur.user_id, LogDataEntry{cur, {0}, 0});
		if ( ret.second )
			continue;

		log_data_t prev;
		{
			//There's an existing element, replace it with the latest one.
			lock_guard<folly::MicroSpinLock> lock(ret.first->second.lock);
			//re-sort the previous-inserted log (just in case we got an out-of-order log)
			if ( ret.first->second.data.timestamp > cur.timestamp ) {
				prev = cur;
				cur = ret.first->second.data;
			} else {
				prev = ret.first->second.data;
			}
			// cout << prev.timestamp.time_since_epoch().count()/1000000 << '<' << cur.timestamp.time_since_epoch().count()/1000000 << '\n';
		}

		//snap the positions to roads


		using namespace boost::geometry;

		projpoint_t prev_pos = project(prev.pos), cur_pos = project(cur.pos);

		const auto travel_dist = distance(prev_pos, cur_pos);
		const auto speed = 1e-3 * travel_dist / chrono::duration_cast<hours_t>(cur.timestamp - prev.timestamp).count();
		//detect jumps and prune them out.
		//jumps/discontinuites are detected by an upper speed threshold.
		//We just hope no care in the philippines exeed 180kph.
		//Anyone exeeding this is most likely overspeeding.
		if ( abs(speed) > 180 ) {
			//break and process the next log.
			continue;
		}

		const auto &results = rn.nearest_edge(cur.pos, 12);
		
		edge_id_t best = 0;
		double best_weight = numeric_limits<double>::min();
		double best_speed_factor = 0;

		projpoint_t snapped_pos;

		for ( auto &&r : results ) {
			auto n = r.first.second - r.first.first;
			const auto edge_length = rn.edgeList[std::get<0>(r.second)].proj_length;
			n.x(n.x()/edge_length);
			n.y(n.y()/edge_length);

			const auto d = cur_pos - r.first.first;

			const auto perp_dist = abs(d % n);
			const auto par_dist = d * n;

			auto dist = perp_dist + (par_dist < 0 ?
				-par_dist :
				(par_dist > edge_length ? par_dist - edge_length : 0) );

			auto dist_weight = (80.0 - dist)/80.0;

			auto dir_weight = ((cur_pos - prev_pos) * n)/travel_dist;

			auto total_weight = dist_weight * dist_factor + dir_weight * heading_factor;

			using namespace boost::geometry;

			if ( total_weight > best_weight ) {
				best_weight = total_weight;
				best = std::get<0>(r.second);
				best_speed_factor = dir_weight;
				
				snapped_pos.x(r.first.first.x() + par_dist * n.x());
				snapped_pos.y(r.first.first.y() + par_dist * n.y());
			}
		}

		const auto s_ll = unproject(snapped_pos);
		cout << user_id << ',' << duration_cast<centiseconds_t>(cur.timestamp.time_since_epoch()).count() << ',' << cur.pos.lon() << ',' << cur.pos.lat() << ',' << s_ll.lon() << ',' << s_ll.lat() << ',' << speed * best_speed_factor << '\n';
	}	
	return 0;
}