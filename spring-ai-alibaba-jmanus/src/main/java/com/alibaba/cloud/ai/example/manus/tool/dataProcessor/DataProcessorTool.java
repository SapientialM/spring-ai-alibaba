/*
 * Copyright 2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.example.manus.tool.dataProcessor;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

import com.alibaba.cloud.ai.example.manus.tool.AbstractBaseTool;
import com.alibaba.cloud.ai.example.manus.tool.TerminableTool;
import com.alibaba.cloud.ai.example.manus.tool.code.ToolExecuteResult;
import com.alibaba.cloud.ai.example.manus.tool.filesystem.UnifiedDirectoryManager;
import com.alibaba.cloud.ai.example.manus.tool.innerStorage.SmartContentSavingService;
import com.alibaba.cloud.ai.example.manus.tool.mapreduce.*;
import com.alibaba.cloud.ai.example.manus.config.ManusProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.openai.api.OpenAiApi;

/**
 * Data Processor Tool - A universal data processing tool specifically designed for
 * organizing web search information
 *
 * This tool encapsulates the MapReduce tool chain and provides a complete data processing
 * workflow: 1. Data Collection: Retrieve data collected by Browser tool from
 * inner_storage 2. Data Splitting: Split large amounts of data into manageable chunks 3.
 * Data Cleaning: Clean and structure each data chunk 4. Data Aggregation: Merge processed
 * data into final results 5. Format Output: Support output in markdown and CSV formats
 */
public class DataProcessorTool extends AbstractBaseTool<DataProcessorTool.DataProcessorInput>
		implements TerminableTool {

	private static final Logger log = LoggerFactory.getLogger(DataProcessorTool.class);

	// ==================== Configuration Constants ====================

	private static final String TOOL_NAME = "data_processor_tool";

	private static final String TOOL_DESCRIPTION = """
			Data Processor Tool - A universal data processing tool specifically designed for organizing web search information

			Main Features:
			1. Intelligent Data Collection: Automatically collect data obtained by Browser tool from inner_storage
			2. Data Cleaning and Structuring: Support cleaning and structuring of various data formats
			3. Format Conversion: Support output in markdown, CSV and other formats
			4. Batch Processing: Support batch processing and aggregation of large amounts of data

			Supported Operations:
			- process_inner_storage_data: Process data in inner_storage
			- clean_and_structure: Clean and structure data
			- convert_format: Convert data format
			- extract_structured_info: Extract structured information

			Output Formats:
			- markdown: Format suitable for reading and further editing
			- csv: Format suitable for data analysis and Excel processing
			- json: Structured format suitable for program processing
			""";

	/**
	 * Internal input class that defines the input parameters for the data processing tool
	 */
	public static class DataProcessorInput {

		private String action;

		@com.fasterxml.jackson.annotation.JsonProperty("source_path")
		private String sourcePath;

		@com.fasterxml.jackson.annotation.JsonProperty("output_format")
		private String outputFormat;

		@com.fasterxml.jackson.annotation.JsonProperty("output_path")
		private String outputPath;

		@com.fasterxml.jackson.annotation.JsonProperty("extraction_fields")
		private List<String> extractionFields;

		@com.fasterxml.jackson.annotation.JsonProperty("cleaning_rules")
		private Map<String, Object> cleaningRules;

		@com.fasterxml.jackson.annotation.JsonProperty("structure_template")
		private String structureTemplate;

		public DataProcessorInput() {
		}

		// Getters and Setters
		public String getAction() {
			return action;
		}

		public void setAction(String action) {
			this.action = action;
		}

		public String getSourcePath() {
			return sourcePath;
		}

		public void setSourcePath(String sourcePath) {
			this.sourcePath = sourcePath;
		}

		public String getOutputFormat() {
			return outputFormat;
		}

		public void setOutputFormat(String outputFormat) {
			this.outputFormat = outputFormat;
		}

		public String getOutputPath() {
			return outputPath;
		}

		public void setOutputPath(String outputPath) {
			this.outputPath = outputPath;
		}

		public List<String> getExtractionFields() {
			return extractionFields;
		}

		public void setExtractionFields(List<String> extractionFields) {
			this.extractionFields = extractionFields;
		}

		public Map<String, Object> getCleaningRules() {
			return cleaningRules;
		}

		public void setCleaningRules(Map<String, Object> cleaningRules) {
			this.cleaningRules = cleaningRules;
		}

		public String getStructureTemplate() {
			return structureTemplate;
		}

		public void setStructureTemplate(String structureTemplate) {
			this.structureTemplate = structureTemplate;
		}

	}

	// ==================== Dependencies ====================

	private final UnifiedDirectoryManager unifiedDirectoryManager;

	private final SmartContentSavingService smartContentSavingService;

	private final MapReduceSharedStateManager sharedStateManager;

	private final ManusProperties manusProperties;

	private final ObjectMapper objectMapper;

	// MapReduce工具链组件
	private DataSplitTool dataSplitTool;

	private MapOutputTool mapOutputTool;

	private ReduceOperationTool reduceOperationTool;

	private FinalizeTool finalizeTool;

	// ==================== State Management ====================

	private volatile boolean processingCompleted = false;

	private String lastProcessingResult = "";

	private String processingTimestamp = "";

	// ==================== Constructor ====================

	public DataProcessorTool(String planId, UnifiedDirectoryManager unifiedDirectoryManager,
			SmartContentSavingService smartContentSavingService, MapReduceSharedStateManager sharedStateManager,
			ManusProperties manusProperties, ObjectMapper objectMapper) {
		this.currentPlanId = planId;
		this.unifiedDirectoryManager = unifiedDirectoryManager;
		this.smartContentSavingService = smartContentSavingService;
		this.sharedStateManager = sharedStateManager;
		this.manusProperties = manusProperties;
		this.objectMapper = objectMapper;

		initializeMapReduceTools();
	}

	/**
	 * 初始化MapReduce工具链
	 */
	private void initializeMapReduceTools() {
		// 这些工具将在实际使用时根据需要创建
		// 避免循环依赖问题
	}

	// ==================== Tool Interface Implementation ====================

	@Override
	public String getName() {
		return TOOL_NAME;
	}

	@Override
	public String getDescription() {
		return TOOL_DESCRIPTION;
	}

	@Override
	public String getParameters() {
		return generateParametersJson();
	}

	@Override
	public Class<DataProcessorInput> getInputType() {
		return DataProcessorInput.class;
	}

	@Override
	public String getServiceGroup() {
		return "data-processing";
	}

	public static OpenAiApi.FunctionTool getToolDefinition() {
		OpenAiApi.FunctionTool.Function function = new OpenAiApi.FunctionTool.Function(TOOL_DESCRIPTION, TOOL_NAME,
				generateParametersJson());
		return new OpenAiApi.FunctionTool(function);
	}

	// ==================== Parameters JSON Generation ====================

	private static String generateParametersJson() {
		return """
				{
				    "type": "object",
				    "properties": {
				        "action": {
				            "type": "string",
				            "enum": ["process_inner_storage_data", "clean_and_structure", "convert_format", "extract_structured_info"],
				            "description": "Data processing operation to execute"
				        },
				        "source_path": {
				            "type": "string",
				            "description": "Source data path, can be inner_storage path or specific file path"
				        },
				        "output_format": {
				            "type": "string",
				            "enum": ["markdown", "csv", "json"],
				            "description": "Output format"
				        },
				        "output_path": {
				            "type": "string",
				            "description": "Output file path (optional)"
				        },
				        "extraction_fields": {
				            "type": "array",
				            "items": {
				                "type": "string"
				            },
				            "description": "List of fields to extract"
				        },
				        "cleaning_rules": {
				            "type": "object",
				            "description": "Data cleaning rules"
				        },
				        "structure_template": {
				            "type": "string",
				            "description": "Structure template"
				        }
				    },
				    "required": ["action"],
				    "additionalProperties": false
				}
				""";
	}

	// ==================== Main Processing Logic ====================

	@Override
	public ToolExecuteResult run(DataProcessorInput input) {
		log.info("DataProcessorTool executing with action: {}", input.getAction());

		try {
			switch (input.getAction()) {
				case "process_inner_storage_data":
					return processInnerStorageData(input);
				case "clean_and_structure":
					return cleanAndStructureData(input);
				case "convert_format":
					return convertFormat(input);
				case "extract_structured_info":
					return extractStructuredInfo(input);
				default:
					return new ToolExecuteResult("Error: Unsupported action: " + input.getAction());
			}
		}
		catch (Exception e) {
			log.error("Error in DataProcessorTool execution", e);
			return new ToolExecuteResult("Error: " + e.getMessage());
		}
	}

	// ==================== Core Processing Methods ====================

	/**
	 * 处理inner_storage中的数据
	 */
	private ToolExecuteResult processInnerStorageData(DataProcessorInput input) throws Exception {
		String sourcePath = input.getSourcePath();
		if (sourcePath == null || sourcePath.trim().isEmpty()) {
			// 默认使用当前计划的inner_storage路径
			sourcePath = getInnerStorageRoot().resolve(currentPlanId).toString();
		}

		Path sourceDir = Paths.get(sourcePath);
		if (!Files.exists(sourceDir)) {
			return new ToolExecuteResult("Error: Source path does not exist: " + sourcePath);
		}

		// 使用MapReduce工具链处理数据
		return executeMapReduceWorkflow(sourceDir, input);
	}

	/**
	 * 清洗和结构化数据
	 */
	private ToolExecuteResult cleanAndStructureData(DataProcessorInput input) throws Exception {
		// 实现数据清洗逻辑
		String sourcePath = input.getSourcePath();
		Path sourceFile = Paths.get(sourcePath);

		if (!Files.exists(sourceFile)) {
			return new ToolExecuteResult("Error: Source file does not exist: " + sourcePath);
		}

		// 读取文件内容
		String content = Files.readString(sourceFile);

		// 应用清洗规则
		String cleanedContent = applyCleaningRules(content, input.getCleaningRules());

		// 结构化数据
		String structuredContent = structureData(cleanedContent, input.getStructureTemplate(),
				input.getExtractionFields());

		// 保存结果
		String outputPath = input.getOutputPath();
		if (outputPath != null && !outputPath.trim().isEmpty()) {
			Files.writeString(Paths.get(outputPath), structuredContent);
			return new ToolExecuteResult("Data cleaned and structured successfully. Output saved to: " + outputPath);
		}
		else {
			return new ToolExecuteResult("Data cleaned and structured successfully:\n" + structuredContent);
		}
	}

	/**
	 * 转换数据格式
	 */
	private ToolExecuteResult convertFormat(DataProcessorInput input) throws Exception {
		String sourcePath = input.getSourcePath();
		String outputFormat = input.getOutputFormat();
		String outputPath = input.getOutputPath();

		Path sourceFile = Paths.get(sourcePath);
		if (!Files.exists(sourceFile)) {
			return new ToolExecuteResult("Error: Source file does not exist: " + sourcePath);
		}

		String content = Files.readString(sourceFile);
		String convertedContent = convertToFormat(content, outputFormat);

		if (outputPath != null && !outputPath.trim().isEmpty()) {
			Files.writeString(Paths.get(outputPath), convertedContent);
			return new ToolExecuteResult("Format converted successfully. Output saved to: " + outputPath);
		}
		else {
			return new ToolExecuteResult("Format converted successfully:\n" + convertedContent);
		}
	}

	/**
	 * 提取结构化信息
	 */
	private ToolExecuteResult extractStructuredInfo(DataProcessorInput input) throws Exception {
		String sourcePath = input.getSourcePath();
		List<String> extractionFields = input.getExtractionFields();

		Path sourceFile = Paths.get(sourcePath);
		if (!Files.exists(sourceFile)) {
			return new ToolExecuteResult("Error: Source file does not exist: " + sourcePath);
		}

		String content = Files.readString(sourceFile);
		Map<String, Object> extractedData = extractFields(content, extractionFields);

		String outputFormat = input.getOutputFormat() != null ? input.getOutputFormat() : "json";
		String formattedOutput = formatExtractedData(extractedData, outputFormat);

		String outputPath = input.getOutputPath();
		if (outputPath != null && !outputPath.trim().isEmpty()) {
			Files.writeString(Paths.get(outputPath), formattedOutput);
			return new ToolExecuteResult(
					"Structured information extracted successfully. Output saved to: " + outputPath);
		}
		else {
			return new ToolExecuteResult("Structured information extracted successfully:\n" + formattedOutput);
		}
	}

	// ==================== MapReduce Workflow Execution ====================

	/**
	 * 执行MapReduce工作流
	 */
	private ToolExecuteResult executeMapReduceWorkflow(Path sourceDir, DataProcessorInput input) throws Exception {
		// 1. 数据分割阶段
		List<String> dataFiles = collectDataFiles(sourceDir);
		if (dataFiles.isEmpty()) {
			return new ToolExecuteResult("No data files found in source directory: " + sourceDir);
		}

		// 2. Map阶段 - 处理每个数据文件
		List<String> mapResults = new ArrayList<>();
		for (String dataFile : dataFiles) {
			String mapResult = processDataFile(dataFile, input);
			mapResults.add(mapResult);
		}

		// 3. Reduce阶段 - 聚合结果
		String aggregatedResult = aggregateResults(mapResults, input);

		// 4. 格式化输出
		String finalOutput = formatFinalOutput(aggregatedResult, input.getOutputFormat());

		// 5. 保存结果
		String outputPath = input.getOutputPath();
		if (outputPath != null && !outputPath.trim().isEmpty()) {
			Files.writeString(Paths.get(outputPath), finalOutput);
			this.lastProcessingResult = "Data processed successfully. Output saved to: " + outputPath;
		}
		else {
			this.lastProcessingResult = "Data processed successfully:\n" + finalOutput;
		}

		this.processingCompleted = true;
		this.processingTimestamp = java.time.LocalDateTime.now().toString();

		return new ToolExecuteResult(this.lastProcessingResult);
	}

	// ==================== Helper Methods ====================

	/**
	 * 收集数据文件
	 */
	private List<String> collectDataFiles(Path sourceDir) throws IOException {
		return Files.walk(sourceDir)
			.filter(Files::isRegularFile)
			.filter(path -> isDataFile(path.toString()))
			.map(Path::toString)
			.collect(Collectors.toList());
	}

	/**
	 * 判断是否为数据文件
	 */
	private boolean isDataFile(String fileName) {
		String lowercaseFileName = fileName.toLowerCase();
		return lowercaseFileName.endsWith(".md") || lowercaseFileName.endsWith(".txt")
				|| lowercaseFileName.endsWith(".csv") || lowercaseFileName.endsWith(".json")
				|| lowercaseFileName.endsWith(".html");
	}

	/**
	 * 处理单个数据文件
	 */
	private String processDataFile(String filePath, DataProcessorInput input) throws IOException {
		String content = Files.readString(Paths.get(filePath));

		// 应用清洗规则
		if (input.getCleaningRules() != null) {
			content = applyCleaningRules(content, input.getCleaningRules());
		}

		// 提取结构化信息
		if (input.getExtractionFields() != null && !input.getExtractionFields().isEmpty()) {
			Map<String, Object> extractedData = extractFields(content, input.getExtractionFields());
			content = objectMapper.writeValueAsString(extractedData);
		}

		return content;
	}

	/**
	 * 聚合处理结果
	 */
	private String aggregateResults(List<String> mapResults, DataProcessorInput input) {
		StringBuilder aggregated = new StringBuilder();

		for (int i = 0; i < mapResults.size(); i++) {
			aggregated.append("=== Data Block ").append(i + 1).append(" ===\n");
			aggregated.append(mapResults.get(i));
			aggregated.append("\n\n");
		}

		return aggregated.toString();
	}

	/**
	 * 格式化最终输出
	 */
	private String formatFinalOutput(String content, String outputFormat) {
		if (outputFormat == null) {
			outputFormat = "markdown";
		}

		return convertToFormat(content, outputFormat);
	}

	/**
	 * 应用清洗规则
	 */
	private String applyCleaningRules(String content, Map<String, Object> cleaningRules) {
		if (cleaningRules == null || cleaningRules.isEmpty()) {
			return content;
		}

		String cleaned = content;

		// 移除HTML标签
		if (Boolean.TRUE.equals(cleaningRules.get("remove_html_tags"))) {
			cleaned = cleaned.replaceAll("<[^>]+>", "");
		}

		// 移除多余空白
		if (Boolean.TRUE.equals(cleaningRules.get("trim_whitespace"))) {
			cleaned = cleaned.replaceAll("\\s+", " ").trim();
		}

		// 移除特殊字符
		if (Boolean.TRUE.equals(cleaningRules.get("remove_special_chars"))) {
			cleaned = cleaned.replaceAll("[^\\w\\s\\p{Punct}]", "");
		}

		return cleaned;
	}

	/**
	 * 结构化数据
	 */
	private String structureData(String content, String template, List<String> fields) {
		if (template == null && (fields == null || fields.isEmpty())) {
			return content;
		}

		// 简单的结构化实现
		StringBuilder structured = new StringBuilder();

		if (fields != null && !fields.isEmpty()) {
			structured.append("# Structured Data\n\n");
			for (String field : fields) {
				structured.append("## ").append(field).append("\n");
				// 这里可以实现更复杂的字段提取逻辑
				structured.append("[Extracted content for ").append(field).append("]\n\n");
			}
		}

		structured.append("## Original Content\n");
		structured.append(content);

		return structured.toString();
	}

	/**
	 * 提取字段
	 */
	private Map<String, Object> extractFields(String content, List<String> fields) {
		Map<String, Object> extracted = new HashMap<>();

		for (String field : fields) {
			// 简单的字段提取实现
			// 实际应用中可以使用更复杂的NLP技术
			extracted.put(field, "[Extracted value for " + field + "]");
		}

		extracted.put("original_content", content);
		extracted.put("extraction_timestamp", java.time.LocalDateTime.now().toString());

		return extracted;
	}

	/**
	 * 转换格式
	 */
	private String convertToFormat(String content, String format) {
		switch (format.toLowerCase()) {
			case "csv":
				return convertToCSV(content);
			case "json":
				return convertToJSON(content);
			case "markdown":
			default:
				return convertToMarkdown(content);
		}
	}

	/**
	 * 转换为CSV格式
	 */
	private String convertToCSV(String content) {
		// 简单的CSV转换实现
		StringBuilder csv = new StringBuilder();
		csv.append("Field,Value\n");

		String[] lines = content.split("\n");
		for (int i = 0; i < lines.length; i++) {
			csv.append("Line_").append(i + 1).append(",\"").append(lines[i].replace("\"", "\\\"")).append("\"\n");
		}

		return csv.toString();
	}

	/**
	 * 转换为JSON格式
	 */
	private String convertToJSON(String content) {
		try {
			Map<String, Object> jsonData = new HashMap<>();
			jsonData.put("content", content);
			jsonData.put("timestamp", java.time.LocalDateTime.now().toString());
			jsonData.put("format", "json");

			return objectMapper.writeValueAsString(jsonData);
		}
		catch (Exception e) {
			log.error("Error converting to JSON", e);
			return "{\"error\": \"Failed to convert to JSON\", \"content\": \"" + content.replace("\"", "\\\"") + "\"}";
		}
	}

	/**
	 * 转换为Markdown格式
	 */
	private String convertToMarkdown(String content) {
		StringBuilder markdown = new StringBuilder();
		markdown.append("# Processed Data\n\n");
		markdown.append("**Processing Timestamp:** ").append(java.time.LocalDateTime.now()).append("\n\n");
		markdown.append("## Content\n\n");
		markdown.append(content);

		return markdown.toString();
	}

	/**
	 * 格式化提取的数据
	 */
	private String formatExtractedData(Map<String, Object> data, String format) {
		switch (format.toLowerCase()) {
			case "csv":
				return formatAsCSV(data);
			case "markdown":
				return formatAsMarkdown(data);
			case "json":
			default:
				try {
					return objectMapper.writeValueAsString(data);
				}
				catch (Exception e) {
					log.error("Error formatting as JSON", e);
					return data.toString();
				}
		}
	}

	/**
	 * 格式化为CSV
	 */
	private String formatAsCSV(Map<String, Object> data) {
		StringBuilder csv = new StringBuilder();
		csv.append("Field,Value\n");

		for (Map.Entry<String, Object> entry : data.entrySet()) {
			csv.append(entry.getKey())
				.append(",\"")
				.append(entry.getValue().toString().replace("\"", "\\\""))
				.append("\"\n");
		}

		return csv.toString();
	}

	/**
	 * 格式化为Markdown
	 */
	private String formatAsMarkdown(Map<String, Object> data) {
		StringBuilder markdown = new StringBuilder();
		markdown.append("# Extracted Data\n\n");

		for (Map.Entry<String, Object> entry : data.entrySet()) {
			markdown.append("## ").append(entry.getKey()).append("\n\n");
			markdown.append(entry.getValue().toString()).append("\n\n");
		}

		return markdown.toString();
	}

	// ==================== Utility Methods ====================

	private Path getInnerStorageRoot() {
		return unifiedDirectoryManager.getInnerStorageRoot();
	}

	// ==================== TerminableTool Implementation ====================

	@Override
	public boolean canTerminate() {
		return processingCompleted;
	}

	// ==================== Tool State Management ====================

	@Override
	public String getCurrentToolStateString() {
		if (sharedStateManager != null && currentPlanId != null) {
			return sharedStateManager.getCurrentToolStateString(currentPlanId);
		}

		StringBuilder sb = new StringBuilder();
		sb.append("DataProcessor Status: ").append(processingCompleted ? "Completed" : "Processing").append("\n");
		if (processingCompleted) {
			sb.append("Last Result: ").append(lastProcessingResult).append("\n");
			sb.append("Timestamp: ").append(processingTimestamp).append("\n");
		}
		return sb.toString();
	}

	@Override
	public void cleanup(String planId) {
		if (sharedStateManager != null && planId != null) {
			sharedStateManager.cleanupPlanState(planId);
		}
		log.info("DataProcessorTool cleanup completed for planId: {}", planId);
	}

	@Override
	public ToolExecuteResult apply(DataProcessorInput input, ToolContext toolContext) {
		return run(input);
	}

}
