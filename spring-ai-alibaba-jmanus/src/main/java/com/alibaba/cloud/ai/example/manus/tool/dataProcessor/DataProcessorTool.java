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
import java.util.Optional;

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
			Data Processor Tool - A universal data processing tool specifically designed for organizing web search information and preparing data for Excel processing

			Main Features:
			1. Intelligent Data Collection: Automatically collect data obtained by Browser tool from inner_storage
			2. Data Cleaning and Structuring: Support cleaning and structuring of various data formats
			3. Format Conversion: Support output in markdown, CSV and JSON formats

			PATH USAGE GUIDELINES:
			- source_path: Use relative paths like 'plan-{planId}' or leave empty for auto-detection
			- output_path: Use relative paths like 'output.csv' or 'results/data.csv'
			- All paths are resolved relative to the inner_storage directory
			- Absolute paths are NOT recommended and may cause security errors
			- When source_path is empty, tool automatically uses current plan's inner_storage path
			4. Batch Processing: Support batch processing and aggregation of large amounts of data
			5. Excel Integration: Optimized CSV output format for seamless integration with ExcelProcessorTool

			Supported Operations:
			- process_inner_storage_data: Process data in inner_storage and output structured data
			- clean_and_structure: Clean and structure data with customizable rules
			- convert_format: Convert data format between markdown, CSV, and JSON
			- extract_structured_info: Extract structured information with field mapping

			Output Formats:
			- markdown: Human-readable format suitable for documentation and further editing
			- csv: Structured tabular format optimized for Excel processing and data analysis
			  * CSV output includes proper headers and data formatting
			  * Compatible with ExcelProcessorTool's read_csv operation
			  * Supports UTF-8 encoding for international characters
			- json: Structured format suitable for program processing and API integration

			Excel Integration Workflow:
			1. Use 'process_inner_storage_data' with output_format='csv' to process browser-collected data
			2. Specify output_path to save CSV file in accessible location (e.g., 'extensions/processed_data.csv')
			3. Use ExcelProcessorTool's 'read_csv' operation to convert CSV to Excel format
			4. ExcelProcessorTool can then perform additional Excel-specific operations (formatting, formulas, etc.)

			Recommended Parameters for Excel Integration:
			- action: 'process_inner_storage_data'
			- output_format: 'csv'
			- output_path: 'extensions/temp_data.csv' (or similar accessible path)
			- extraction_fields: [list of column names for structured output]
			- structure_template: 'column1,column2,column3' (CSV header format)
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
				        "description": "Source path for data processing. Use relative paths like 'plan-{planId}' or leave empty for auto-detection. Resolved relative to inner_storage directory."
				    },
				        "output_format": {
				            "type": "string",
				            "enum": ["markdown", "csv", "json"],
				            "description": "Output format"
				        },
				        "output_path": {
				        "type": "string",
				        "description": "Output path for processed data. Use relative paths like 'output.csv' or 'results/data.csv'. Resolved relative to inner_storage directory."
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
		try {
			// Validate input
			if (input == null) {
				log.error("DataProcessorTool: Input is null");
				return new ToolExecuteResult("Error: Input cannot be null");
			}

			String action = input.getAction();
			if (action == null || action.trim().isEmpty()) {
				log.error("DataProcessorTool: Action is null or empty");
				return new ToolExecuteResult("Error: Action is required");
			}

			log.info("DataProcessorTool executing with action: {}", action);

			switch (action) {
				case "process_inner_storage_data":
					return processInnerStorageData(input);
				case "clean_and_structure":
					return cleanAndStructureData(input);
				case "convert_format":
					return convertFormat(input);
				case "extract_structured_info":
					return extractStructuredInfo(input);
				default:
					log.error("DataProcessorTool: Unsupported action: {}", action);
					return new ToolExecuteResult("Error: Unsupported action: " + action
							+ ". Supported actions: process_inner_storage_data, clean_and_structure, convert_format, extract_structured_info");
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
		try {
			log.info("Processing inner storage data");

			// Validate and normalize source path
			String sourcePath = validateAndNormalizePath(input.getSourcePath(), false);

			// If no source path provided, try to infer the best one
			if (sourcePath == null || sourcePath.isEmpty()) {
				sourcePath = inferBestSourcePath();
				if (sourcePath == null) {
					log.error("No source path specified and unable to auto-detect");
					return new ToolExecuteResult("Error: No source path specified and unable to auto-detect. "
							+ "Available paths in inner_storage: " + listAvailablePaths(getInnerStorageRoot()) + ". "
							+ "Please specify source_path parameter with a relative path like 'plan-{planId}'.");
				}
				log.info("Auto-detected source path: {}", sourcePath);
			}

			Path resolvedSourcePath;
			if (Paths.get(sourcePath).isAbsolute()) {
				// Validate absolute path must be within inner_storage directory
				Path innerStorageRoot = getInnerStorageRoot();
				Path absolutePath = Paths.get(sourcePath);
				if (!absolutePath.startsWith(innerStorageRoot)) {
					return new ToolExecuteResult("Error: Absolute path must be within inner_storage directory. "
							+ "Expected path under: " + innerStorageRoot + ", but got: " + absolutePath + ". "
							+ "Please use relative paths like 'plan-{planId}' or leave source_path empty for auto-detection.");
				}
				resolvedSourcePath = absolutePath;
			}
			else {
				// Relative path resolved to inner_storage directory
				resolvedSourcePath = getInnerStorageRoot().resolve(sourcePath);
			}

			if (!Files.exists(resolvedSourcePath)) {
				log.error("Source path does not exist: {}", resolvedSourcePath);
				return new ToolExecuteResult("Error: Source path does not exist: " + resolvedSourcePath + ". "
						+ "Expected path: " + sourcePath + ". "
						+ "Please use relative paths like 'plan-{planId}' or leave source_path empty for auto-detection. "
						+ "Available paths in inner_storage: " + listAvailablePaths(getInnerStorageRoot()));
			}

			if (!Files.isDirectory(resolvedSourcePath)) {
				log.error("Source path is not a directory: {}", resolvedSourcePath);
				return new ToolExecuteResult("Error: Source path is not a directory: " + resolvedSourcePath + ". "
						+ "Expected a directory containing data files.");
			}

			// Check if directory contains any data files
			List<String> dataFiles = collectDataFiles(resolvedSourcePath);
			if (dataFiles.isEmpty()) {
				log.warn("No data files found in source directory: {}", resolvedSourcePath);
				return new ToolExecuteResult("Warning: No data files found in source directory: " + resolvedSourcePath
						+ ". " + "Supported file types: .md, .txt, .csv, .json, .html");
			}

			log.info("Found {} data files in source directory", dataFiles.size());
			return executeMapReduceWorkflow(resolvedSourcePath, input);

		}
		catch (IllegalArgumentException e) {
			return new ToolExecuteResult("Error: Invalid path parameter - " + e.getMessage());
		}
	}

	/**
	 * 清洗和结构化数据
	 */
	private ToolExecuteResult cleanAndStructureData(DataProcessorInput input) throws Exception {
		try {
			log.info("Cleaning and structuring data");

			// 实现数据清洗逻辑
			String sourcePath = validateAndNormalizePath(input.getSourcePath(), false);
			if (sourcePath == null || sourcePath.isEmpty()) {
				log.error("Source path is required for clean and structure operation");
				return new ToolExecuteResult("Error: Source path is required for clean and structure operation");
			}

			Path sourceFile = getInnerStorageRoot().resolve(sourcePath);

			if (!Files.exists(sourceFile)) {
				log.error("Source file does not exist: {}", sourceFile);
				return new ToolExecuteResult("Error: Source file does not exist: " + sourceFile);
			}

			if (!Files.isRegularFile(sourceFile)) {
				log.error("Source path is not a regular file: {}", sourceFile);
				return new ToolExecuteResult("Error: Source path is not a regular file: " + sourceFile);
			}

			// 读取文件内容
			String content = Files.readString(sourceFile);

			// 应用清洗规则
			String cleanedContent = applyCleaningRules(content, input.getCleaningRules());

			// 结构化数据
			String structuredContent = structureData(cleanedContent, input.getStructureTemplate(),
					input.getExtractionFields());

			// 保存结果
			String outputPath = validateAndNormalizePath(input.getOutputPath(), true);
			if (outputPath != null && !outputPath.trim().isEmpty()) {
				Path resolvedOutputPath = getInnerStorageRoot().resolve(outputPath);
				// Ensure output directory exists
				Files.createDirectories(resolvedOutputPath.getParent());
				Files.writeString(resolvedOutputPath, structuredContent);
				log.info("Data cleaned and structured successfully. Output saved to: {}", resolvedOutputPath);
				return new ToolExecuteResult(
						"Data cleaned and structured successfully. Output saved to: " + resolvedOutputPath);
			}
			else {
				log.info("Data cleaned and structured successfully, returning content directly");
				return new ToolExecuteResult("Data cleaned and structured successfully:\n" + structuredContent);
			}
		}
		catch (IllegalArgumentException e) {
			log.error("Invalid path parameter: {}", e.getMessage());
			return new ToolExecuteResult("Error: Invalid path parameter - " + e.getMessage());
		}
	}

	/**
	 * 转换数据格式
	 */
	private ToolExecuteResult convertFormat(DataProcessorInput input) throws Exception {
		try {
			log.info("Converting data format");

			String sourcePath = validateAndNormalizePath(input.getSourcePath(), false);
			if (sourcePath == null || sourcePath.isEmpty()) {
				log.error("Source path is required for format conversion");
				return new ToolExecuteResult("Error: Source path is required for format conversion");
			}

			String outputFormat = input.getOutputFormat();
			if (outputFormat == null || outputFormat.trim().isEmpty()) {
				log.error("Output format is required for format conversion");
				return new ToolExecuteResult("Error: Output format is required for format conversion");
			}

			String outputPath = validateAndNormalizePath(input.getOutputPath(), true);

			Path sourceFile = getInnerStorageRoot().resolve(sourcePath);
			if (!Files.exists(sourceFile)) {
				log.error("Source file does not exist: {}", sourceFile);
				return new ToolExecuteResult("Error: Source file does not exist: " + sourceFile);
			}

			if (!Files.isRegularFile(sourceFile)) {
				log.error("Source path is not a regular file: {}", sourceFile);
				return new ToolExecuteResult("Error: Source path is not a regular file: " + sourceFile);
			}

			String content = Files.readString(sourceFile);
			String convertedContent = convertToFormat(content, outputFormat);

			if (outputPath != null && !outputPath.trim().isEmpty()) {
				Path resolvedOutputPath = getInnerStorageRoot().resolve(outputPath);
				// Ensure output directory exists
				Files.createDirectories(resolvedOutputPath.getParent());
				Files.writeString(resolvedOutputPath, convertedContent);
				log.info("Format converted successfully. Output saved to: {}", resolvedOutputPath);
				return new ToolExecuteResult("Format converted successfully. Output saved to: " + resolvedOutputPath);
			}
			else {
				log.info("Format converted successfully, returning content directly");
				return new ToolExecuteResult("Format converted successfully:\n" + convertedContent);
			}
		}
		catch (IllegalArgumentException e) {
			log.error("Invalid path parameter: {}", e.getMessage());
			return new ToolExecuteResult("Error: Invalid path parameter - " + e.getMessage());
		}
	}

	/**
	 * 提取结构化信息
	 */
	private ToolExecuteResult extractStructuredInfo(DataProcessorInput input) throws Exception {
		try {
			log.info("Extracting structured information");

			String sourcePath = validateAndNormalizePath(input.getSourcePath(), false);
			if (sourcePath == null || sourcePath.isEmpty()) {
				log.error("Source path is required for structured info extraction");
				return new ToolExecuteResult("Error: Source path is required for structured info extraction");
			}

			List<String> extractionFields = input.getExtractionFields();
			if (extractionFields == null || extractionFields.isEmpty()) {
				log.error("Extraction fields are required for structured info extraction");
				return new ToolExecuteResult("Error: Extraction fields are required for structured info extraction");
			}

			Path sourceFile = getInnerStorageRoot().resolve(sourcePath);
			if (!Files.exists(sourceFile)) {
				log.error("Source file does not exist: {}", sourceFile);
				return new ToolExecuteResult("Error: Source file does not exist: " + sourceFile);
			}

			if (!Files.isRegularFile(sourceFile)) {
				log.error("Source path is not a regular file: {}", sourceFile);
				return new ToolExecuteResult("Error: Source path is not a regular file: " + sourceFile);
			}

			String content = Files.readString(sourceFile);
			Map<String, Object> extractedData = extractFields(content, extractionFields);

			String outputFormat = input.getOutputFormat() != null ? input.getOutputFormat() : "json";
			String formattedOutput = formatExtractedData(extractedData, outputFormat);

			String outputPath = validateAndNormalizePath(input.getOutputPath(), true);
			if (outputPath != null && !outputPath.trim().isEmpty()) {
				Path resolvedOutputPath = getInnerStorageRoot().resolve(outputPath);
				// Ensure output directory exists
				Files.createDirectories(resolvedOutputPath.getParent());
				Files.writeString(resolvedOutputPath, formattedOutput);
				log.info("Structured information extracted successfully. Output saved to: {}", resolvedOutputPath);
				return new ToolExecuteResult(
						"Structured information extracted successfully. Output saved to: " + resolvedOutputPath);
			}
			else {
				log.info("Structured information extracted successfully, returning content directly");
				return new ToolExecuteResult("Structured information extracted successfully:\n" + formattedOutput);
			}
		}
		catch (IllegalArgumentException e) {
			log.error("Invalid path parameter: {}", e.getMessage());
			return new ToolExecuteResult("Error: Invalid path parameter - " + e.getMessage());
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

		// 5. Save results with intelligent path handling
		try {
			String outputPath = validateAndNormalizePath(input.getOutputPath(), true);
			if (outputPath != null && !outputPath.isEmpty()) {
				Path resolvedOutputPath;
				if (Paths.get(outputPath).isAbsolute()) {
					// Validate absolute path must be within inner_storage directory
					Path innerStorageRoot = getInnerStorageRoot();
					Path absolutePath = Paths.get(outputPath);
					if (!absolutePath.startsWith(innerStorageRoot)) {
						return new ToolExecuteResult("Error: Absolute path must be within inner_storage directory. "
								+ "Expected path under: " + innerStorageRoot + ", but got: " + absolutePath + ". "
								+ "Please use relative paths like 'output.csv' or 'results/data.csv'.");
					}
					resolvedOutputPath = absolutePath;
				}
				else {
					// Relative path resolved to inner_storage directory
					resolvedOutputPath = getInnerStorageRoot().resolve(outputPath);
				}

				// Ensure output directory exists
				Files.createDirectories(resolvedOutputPath.getParent());

				Files.writeString(resolvedOutputPath, finalOutput);
				this.lastProcessingResult = "Data processed successfully. Output saved to: " + resolvedOutputPath;
				log.info("Data processing completed. Output saved to: {}", resolvedOutputPath);
			}
			else {
				this.lastProcessingResult = "Data processed successfully:\n" + finalOutput;
				log.info("Data processing completed. No output file specified, returning results directly.");
			}
		}
		catch (IllegalArgumentException e) {
			return new ToolExecuteResult("Error: Invalid output path - " + e.getMessage());
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
		try {
			log.debug("Collecting data files from directory: {}", sourceDir);

			if (!Files.exists(sourceDir)) {
				log.warn("Source directory does not exist: {}", sourceDir);
				return new ArrayList<>();
			}

			if (!Files.isDirectory(sourceDir)) {
				log.warn("Source path is not a directory: {}", sourceDir);
				return new ArrayList<>();
			}

			List<String> dataFiles = Files.walk(sourceDir)
				.filter(Files::isRegularFile)
				.filter(path -> isDataFile(path.toString()))
				.map(Path::toString)
				.collect(Collectors.toList());

			log.debug("Found {} data files", dataFiles.size());
			return dataFiles;
		}
		catch (IOException e) {
			log.error("Error collecting data files from directory: {}", sourceDir, e);
			throw e;
		}
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
		try {
			log.debug("Processing data file: {}", filePath);

			Path file = Paths.get(filePath);
			if (!Files.exists(file)) {
				log.warn("Data file does not exist: {}", filePath);
				return "[File not found: " + filePath + "]";
			}

			if (!Files.isRegularFile(file)) {
				log.warn("Path is not a regular file: {}", filePath);
				return "[Not a regular file: " + filePath + "]";
			}

			String content = Files.readString(file);
			log.debug("Successfully read file content, length: {}", content.length());

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
		catch (IOException e) {
			log.error("Error processing data file: {}", filePath, e);
			return "[Error reading file " + filePath + ": " + e.getMessage() + "]";
		}
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
		try {
			log.debug("Converting content to CSV format");
			// Enhanced CSV conversion with better structure
			StringBuilder csv = new StringBuilder();
			csv.append("Field,Value\n");

			String[] lines = content.split("\n");
			for (int i = 0; i < lines.length; i++) {
				String line = lines[i].trim();
				if (!line.isEmpty()) {
					// Escape quotes and handle CSV formatting properly
					String escapedLine = line.replace("\"", "\"\"");
					csv.append("Line_").append(i + 1).append(",\"").append(escapedLine).append("\"\n");
				}
			}

			log.debug("CSV conversion completed successfully");
			return csv.toString();
		}
		catch (Exception e) {
			log.error("Error converting to CSV format", e);
			return "Field,Value\n\"Error\",\"Failed to convert content to CSV: " + e.getMessage() + "\"\n";
		}
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

	/**
	 * List available paths in the given directory for error reporting
	 */
	private String listAvailablePaths(Path directory) {
		try {
			if (!Files.exists(directory) || !Files.isDirectory(directory)) {
				return "No directories found";
			}

			List<String> availablePaths = Files.list(directory)
				.filter(Files::isDirectory)
				.map(path -> path.getFileName().toString())
				.limit(10)
				.collect(Collectors.toList());

			if (availablePaths.isEmpty()) {
				return "No directories found";
			}

			return String.join(", ", availablePaths);
		}
		catch (IOException e) {
			return "Unable to list directories: " + e.getMessage();
		}
	}

	/**
	 * Intelligently infer the best source path when not specified Priority: current plan
	 * directory > most recent directory > first available directory
	 */
	private String inferBestSourcePath() {
		try {
			Path innerStorageRoot = getInnerStorageRoot();
			if (!Files.exists(innerStorageRoot) || !Files.isDirectory(innerStorageRoot)) {
				return null;
			}

			// Priority 1: Current plan directory
			String currentPlanPath = "plan-" + currentPlanId;
			Path currentPlanDir = innerStorageRoot.resolve(currentPlanPath);
			if (Files.exists(currentPlanDir) && Files.isDirectory(currentPlanDir)) {
				return currentPlanPath;
			}

			// Priority 2: Most recent plan directory based on modification time
			Optional<Path> mostRecentPlan = Files.list(innerStorageRoot)
				.filter(Files::isDirectory)
				.filter(path -> path.getFileName().toString().startsWith("plan-"))
				.max((p1, p2) -> {
					try {
						return Files.getLastModifiedTime(p1).compareTo(Files.getLastModifiedTime(p2));
					}
					catch (IOException e) {
						return 0;
					}
				});

			if (mostRecentPlan.isPresent()) {
				return mostRecentPlan.get().getFileName().toString();
			}

			// Priority 3: First available directory
			Optional<Path> firstDir = Files.list(innerStorageRoot).filter(Files::isDirectory).findFirst();

			return firstDir.map(path -> path.getFileName().toString()).orElse(null);

		}
		catch (IOException e) {
			log.warn("Failed to infer best source path: {}", e.getMessage());
			return null;
		}
	}

	/**
	 * Validate and normalize path parameter
	 * @param inputPath Raw path input from user
	 * @param isOutputPath Whether this is an output path (affects validation rules)
	 * @return Validated and normalized path relative to inner_storage
	 */
	private String validateAndNormalizePath(String inputPath, boolean isOutputPath) {
		if (inputPath == null || inputPath.trim().isEmpty()) {
			log.debug("Path is null or empty, returning null");
			return null;
		}

		String normalizedPath = inputPath.trim();
		log.debug("Validating path: {} (isOutputPath: {})", normalizedPath, isOutputPath);

		// Remove leading/trailing slashes for consistency
		normalizedPath = normalizedPath.replaceAll("^/+|/+$", "");

		// Validate path doesn't contain dangerous patterns
		if (normalizedPath.contains("..") || normalizedPath.contains("~")) {
			log.error("Path contains invalid characters: {}", normalizedPath);
			throw new IllegalArgumentException("Path contains invalid characters (.. or ~): " + normalizedPath);
		}

		// Validate path doesn't contain null bytes or other dangerous characters
		if (normalizedPath.contains("\0") || normalizedPath.matches(".*[<>:|?*].*")) {
			log.error("Path contains forbidden characters: {}", normalizedPath);
			throw new IllegalArgumentException("Path contains forbidden characters: " + normalizedPath);
		}

		// For output paths, ensure proper file extension
		if (isOutputPath && !normalizedPath.isEmpty()) {
			if (!normalizedPath.contains(".")) {
				// Auto-append .csv extension if no extension provided
				normalizedPath += ".csv";
				log.debug("Auto-appended .csv extension: {}", normalizedPath);
			}
		}

		log.debug("Normalized path: {}", normalizedPath);
		return normalizedPath;
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
