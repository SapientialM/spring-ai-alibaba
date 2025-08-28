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

import com.alibaba.cloud.ai.example.manus.config.ManusProperties;
import com.alibaba.cloud.ai.example.manus.tool.code.ToolExecuteResult;
import com.alibaba.cloud.ai.example.manus.tool.filesystem.UnifiedDirectoryManager;
import com.alibaba.cloud.ai.example.manus.tool.innerStorage.SmartContentSavingService;
import com.alibaba.cloud.ai.example.manus.tool.mapreduce.MapReduceSharedStateManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * DataProcessorTool test class
 */
class DataProcessorToolTest {

	@Mock
	private UnifiedDirectoryManager unifiedDirectoryManager;

	@Mock
	private SmartContentSavingService smartContentSavingService;

	@Mock
	private MapReduceSharedStateManager sharedStateManager;

	@Mock
	private ManusProperties manusProperties;

	private ObjectMapper objectMapper;

	private DataProcessorTool dataProcessorTool;

	@TempDir
	Path tempDir;

	private static final String TEST_PLAN_ID = "test-plan-123";

	@BeforeEach
	void setUp() {
		MockitoAnnotations.openMocks(this);
		objectMapper = new ObjectMapper();

		// Mock UnifiedDirectoryManager to return temp directory
		when(unifiedDirectoryManager.getInnerStorageRoot()).thenReturn(tempDir);

		dataProcessorTool = new DataProcessorTool(TEST_PLAN_ID, unifiedDirectoryManager, smartContentSavingService,
				sharedStateManager, manusProperties, objectMapper);
	}

	@Test
	void testGetName() {
		assertEquals("data_processor_tool", dataProcessorTool.getName());
	}

	@Test
	void testGetDescription() {
		String description = dataProcessorTool.getDescription();
		assertNotNull(description);
		assertTrue(description.contains("Data Processor Tool"));
		assertTrue(description.contains("web search information"));
	}

	@Test
	void testGetInputType() {
		assertEquals(DataProcessorTool.DataProcessorInput.class, dataProcessorTool.getInputType());
	}

	@Test
	void testGetServiceGroup() {
		assertEquals("data-processing", dataProcessorTool.getServiceGroup());
	}

	@Test
	void testCleanAndStructureData() throws IOException {
		// 创建测试文件
		Path testFile = tempDir.resolve("test_data.txt");
		String testContent = "<html><body>  Test content with HTML tags  </body></html>";
		Files.writeString(testFile, testContent);

		// 创建输入参数
		DataProcessorTool.DataProcessorInput input = new DataProcessorTool.DataProcessorInput();
		input.setAction("clean_and_structure");
		input.setSourcePath(testFile.toString());
		input.setOutputFormat("markdown");

		// 设置清洗规则
		Map<String, Object> cleaningRules = new HashMap<>();
		cleaningRules.put("remove_html_tags", true);
		cleaningRules.put("trim_whitespace", true);
		input.setCleaningRules(cleaningRules);

		// 执行测试
		ToolExecuteResult result = dataProcessorTool.run(input);

		// 验证结果
		assertNotNull(result);
		assertNotNull(result.getOutput());
		assertTrue(result.getOutput().contains("Data cleaned and structured successfully"));
	}

	@Test
	void testConvertFormat() throws IOException {
		// 创建测试文件
		Path testFile = tempDir.resolve("test_data.md");
		String testContent = "# Test Title\n\nTest content for format conversion.";
		Files.writeString(testFile, testContent);

		// 创建输出文件路径
		Path outputFile = tempDir.resolve("output.csv");

		// 创建输入参数
		DataProcessorTool.DataProcessorInput input = new DataProcessorTool.DataProcessorInput();
		input.setAction("convert_format");
		input.setSourcePath(testFile.toString());
		input.setOutputFormat("csv");
		input.setOutputPath(outputFile.toString());

		// 执行测试
		ToolExecuteResult result = dataProcessorTool.run(input);

		// 验证结果
		assertNotNull(result);
		assertTrue(result.getOutput().contains("Format converted successfully"));
		assertTrue(Files.exists(outputFile));

		// 验证输出文件内容
		String outputContent = Files.readString(outputFile);
		assertTrue(outputContent.contains("Field,Value"));
	}

	@Test
	void testExtractStructuredInfo() throws IOException {
		// 创建测试文件
		Path testFile = tempDir.resolve("research_paper.txt");
		String testContent = "Title: Machine Learning in Healthcare\n" + "Authors: John Doe, Jane Smith\n"
				+ "Date: 2025-01-18\n"
				+ "Abstract: This paper discusses the application of machine learning in healthcare.";
		Files.writeString(testFile, testContent);

		// 创建输入参数
		DataProcessorTool.DataProcessorInput input = new DataProcessorTool.DataProcessorInput();
		input.setAction("extract_structured_info");
		input.setSourcePath(testFile.toString());
		input.setOutputFormat("json");
		input.setExtractionFields(Arrays.asList("title", "authors", "date", "abstract"));

		// 执行测试
		ToolExecuteResult result = dataProcessorTool.run(input);

		// 验证结果
		assertNotNull(result);
		assertTrue(result.getOutput().contains("Structured information extracted successfully"));
	}

	@Test
	void testProcessInnerStorageDataWithNonExistentPath() {
		// 创建输入参数，使用不存在的路径
		DataProcessorTool.DataProcessorInput input = new DataProcessorTool.DataProcessorInput();
		input.setAction("process_inner_storage_data");
		input.setSourcePath("/non/existent/path");

		// 执行测试
		ToolExecuteResult result = dataProcessorTool.run(input);

		// 验证错误处理
		assertNotNull(result);
		assertTrue(result.getOutput().contains("Error: Source path does not exist"));
	}

	@Test
	void testProcessInnerStorageDataWithDefaultPath() throws IOException {
		// 创建默认inner_storage目录结构
		Path planDir = tempDir.resolve(TEST_PLAN_ID);
		Files.createDirectories(planDir);

		// 创建测试数据文件
		Path dataFile = planDir.resolve("test_data.md");
		String testContent = "# Test Data\n\nThis is test content for processing.";
		Files.writeString(dataFile, testContent);

		// 创建输入参数（不指定source_path，使用默认路径）
		DataProcessorTool.DataProcessorInput input = new DataProcessorTool.DataProcessorInput();
		input.setAction("process_inner_storage_data");
		input.setOutputFormat("markdown");

		// 执行测试
		ToolExecuteResult result = dataProcessorTool.run(input);

		// 验证结果
		assertNotNull(result);
		assertTrue(result.getOutput().contains("Data processed successfully"));
	}

	@Test
	void testUnsupportedAction() {
		// 创建输入参数，使用不支持的操作
		DataProcessorTool.DataProcessorInput input = new DataProcessorTool.DataProcessorInput();
		input.setAction("unsupported_action");

		// 执行测试
		ToolExecuteResult result = dataProcessorTool.run(input);

		// 验证错误处理
		assertNotNull(result);
		assertTrue(result.getOutput().contains("Error: Unsupported action"));
	}

	@Test
	void testCanTerminate() {
		// 初始状态应该不能终止
		assertFalse(dataProcessorTool.canTerminate());

		// 执行一个简单的操作
		DataProcessorTool.DataProcessorInput input = new DataProcessorTool.DataProcessorInput();
		input.setAction("unsupported_action");
		dataProcessorTool.run(input);

		// 执行后状态可能会改变，但这取决于具体实现
		// 这里主要测试方法是否可调用
		assertNotNull(dataProcessorTool.canTerminate());
	}

	@Test
	void testGetCurrentToolStateString() {
		// Mock the sharedStateManager to return a valid state string
		when(sharedStateManager.getCurrentToolStateString(TEST_PLAN_ID))
				.thenReturn("DataProcessor Status: Processing\nCurrent operation: test");
		
		String state = dataProcessorTool.getCurrentToolStateString();
		assertNotNull(state);
		assertTrue(state.contains("DataProcessor Status"));
	}

	@Test
	void testCleanup() {
		// 测试清理方法不抛出异常
		assertDoesNotThrow(() -> dataProcessorTool.cleanup(TEST_PLAN_ID));
	}

	@Test
	void testDataProcessorInputGettersAndSetters() {
		DataProcessorTool.DataProcessorInput input = new DataProcessorTool.DataProcessorInput();

		// 测试所有getter和setter
		input.setAction("test_action");
		assertEquals("test_action", input.getAction());

		input.setSourcePath("/test/path");
		assertEquals("/test/path", input.getSourcePath());

		input.setOutputFormat("json");
		assertEquals("json", input.getOutputFormat());

		input.setOutputPath("/output/path");
		assertEquals("/output/path", input.getOutputPath());

		input.setExtractionFields(Arrays.asList("field1", "field2"));
		assertEquals(Arrays.asList("field1", "field2"), input.getExtractionFields());

		Map<String, Object> rules = new HashMap<>();
		rules.put("test_rule", true);
		input.setCleaningRules(rules);
		assertEquals(rules, input.getCleaningRules());

		input.setStructureTemplate("template");
		assertEquals("template", input.getStructureTemplate());
	}

	@Test
	void testParametersJsonGeneration() {
		String parameters = dataProcessorTool.getParameters();
		assertNotNull(parameters);
		assertTrue(parameters.contains("process_inner_storage_data"));
		assertTrue(parameters.contains("clean_and_structure"));
		assertTrue(parameters.contains("convert_format"));
		assertTrue(parameters.contains("extract_structured_info"));
		assertTrue(parameters.contains("markdown"));
		assertTrue(parameters.contains("csv"));
		assertTrue(parameters.contains("json"));
	}

	@Test
	void testToolDefinition() {
		var toolDefinition = DataProcessorTool.getToolDefinition();
		assertNotNull(toolDefinition);
		// Test that the tool definition is properly created
		// The actual function properties are encapsulated within the FunctionTool
	}

}
