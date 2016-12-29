package us.dot.its.jpo.ode.plugin.j2735.oss;

import us.dot.its.jpo.ode.j2735.dsrc.PathHistory;
import us.dot.its.jpo.ode.plugin.j2735.J2735PathHistory;

public class OssPathHistory {

	public static J2735PathHistory genericPathHistory(PathHistory pathHistory) {
		J2735PathHistory ph = new J2735PathHistory();
		
		
		ph.crumbData = OssPathHistoryPointList.genericPathHistoryPointList(pathHistory.crumbData);
		ph.currGNSSstatus = OssBitString.genericBitString(pathHistory.currGNSSstatus);
		ph.initialPosition = OssFullPositionVector.genericFullPositionVector(pathHistory.initialPosition);
		
		return ph ;
	}

}
