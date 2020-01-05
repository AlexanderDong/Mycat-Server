 package com.chuxing365.route.function;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.model.rule.RuleAlgorithm;
import io.mycat.route.function.AbstractPartitionAlgorithm;

/**   
 * 描述: 根据传入的前4位值定位分片<br> {区域编码2位}{虚拟库号两位}
 * 1,根据区域编码虚拟库号拼出库名称<br>
 * 2,根据库名称找到mycat配置的分片库标识（schema.xml中 dataNode元素name对应的database，网站的建库规则决定了database不会重复）<br>
 * 3,根据配置的分片库标识顺序定位要返回的值
 * @author: dongChao
 * @date:Jan 4, 2020 7:16:27 PM     
 */
public class PartitionByAreacodeVnode extends AbstractPartitionAlgorithm implements RuleAlgorithm{

	private static final long serialVersionUID = -2576531945998214994L;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(PartitionByAreacodeVnode.class);
	
	private static final String AREACODE_CONFIG = "78:nmt,76:nmd";
	
	private static final String DATABASE_CONFIG = "nmd_u1wt:nmd_u1wt,nmd_u2wt:nmd_u2wt";
	
	/** 当前区域编码对应的省标识 */
	private Map<String, String> areaCodeMap = new HashMap<String, String>();
	
	private Map<String, String> dataBaseMap = new HashMap<String, String>();
	
	private Map<String, Integer> dataNodeMap = new HashMap<String, Integer>();
	
	@Override
	public Integer calculate(String columnValue) {
		Integer node = 0;
		if(dataNodeMap.size() == 0) {
			return node;
		}
		try {
			String database = getDataBase(columnValue);
			// 根据database找到mycat配置的datanode
			String dataNode = dataBaseMap.get(database);
			// 当前node的库索引
			node = dataNodeMap.get(dataNode);
		}catch(Exception e) {
			LOGGER.error("定位库索引异常" + columnValue, e);
		}
		node = null == node ? 0 : node;
		return node;
	}
	
	public void setAreaCodeConfig(String areaCodeConfig) {
		if(StringUtils.isEmpty(areaCodeConfig)) {
			areaCodeConfig = AREACODE_CONFIG;
		}
		String[] configArray = areaCodeConfig.split(",");
		for(int i=0; i<configArray.length; i++) {
			if(StringUtils.isNotEmpty(configArray[i])) {
				String[] tmp = configArray[i].split(":");
				areaCodeMap.put(tmp[0], tmp[1]);
			}
		}
	}

	public void setDataBaseConfig(String dataBaseConfig) {
		if(StringUtils.isEmpty(dataBaseConfig)) {
			dataBaseConfig = DATABASE_CONFIG;
		}
		String[] configArray = dataBaseConfig.split(",");
		for(int i=0; i<configArray.length; i++) {
			if(StringUtils.isNotEmpty(configArray[i])) {
				String[] tmp = configArray[i].split(":");
				dataBaseMap.put(tmp[0], tmp[1]);
			}
		}
	}
	
	public void setDataNode(String dataNode) {
		if(StringUtils.isNotEmpty(dataNode)) {
			String[] configArray = dataNode.split(",");
			for(int i=0; i<configArray.length; i++) {
				if(StringUtils.isNotEmpty(configArray[i])) {
					dataNodeMap.put(configArray[i], i);
				}
			}
		}
	}
	
	private String getDataBase(String columnValue) {
		String database = null;
		if(columnValue.length() > 3) {
			String areacode = columnValue.substring(0, 2);
			Integer vnode = Integer.valueOf(columnValue.substring(2, 4));
			database = areaCodeMap.get(areacode) + "_" + (vnode < 51 ? "u1wt" : "u2wt");
		}
		return database;
	}
	
}
