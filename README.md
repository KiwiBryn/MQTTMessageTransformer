# NanoMQTTMessageTransformer

A collection of MQTT message transformation, monitoring, and analytics examples built with .NET.

This repository explores techniques for:

* MQTT message ingestion and republishing
* Message transformation pipelines
* Dynamic C# script-based transformations
* MQTT loopback testing
* Device liveness monitoring
* Spike detection
* Change-point detection
* ML.NET-based analytics for MQTT telemetry

## Solution Structure

The repository contains several sample projects focused on different MQTT processing scenarios. [\[github.com\]](https://github.com/KiwiBryn/NanoMQTTMessageTransformer/tree/main/src)

```text
src/
├── HiveMQClient/
├── IMessageTransformer/
├── LightGBMBinaryClassificationTrainer/
├── LightGBMDataGeneratorEvT/
├── LightGBMMultiClassClassificationTrainer/
├── LightGBMRegressionTrainer/
├── MQTTChangePointDetection/
├── MQTTCLientCodeLoopback/
├── MQTTChangePointDetection/
├── MQTTCLientCodeLoopback/
├── MQTTClientCSScriptDynamicLoadingLoopback/
├── MQTTClientCSScriptLoopback/
├── MQTTDeviceSimulator/
├── MQTTLightGBM/
├── MQTTLivenessMonitor/
├── MQTTSpikeDetection/
├── MQTTSRCNNSpikeDetection/
├── NFMQTTUltrasonicRanger/
└── MQTTMachineLearningWithMLNET.slnx
```

## Key Features

### Dynamic Message Transformation

Use C# scripts to transform MQTT payloads without rebuilding applications.

### Anomaly Detection

Detect unusual telemetry patterns including:

* Sudden spikes
* Sensor drift
* Change points in streaming data

### Device Monitoring

Monitor MQTT-connected devices and identify offline or unresponsive nodes.

### Machine Learning Integration

Experiment with ML.NET models for real-time telemetry analysis and event detection.

## Example Use Cases

* IoT sensor monitoring
* Edge-to-cloud telemetry processing
* MQTT topic routing
* Sensor anomaly detection
* Industrial monitoring
* Rapid prototyping of MQTT transformations

## Architecture

```text
MQTT Publisher
       │
       ▼
  MQTT Broker
       │
       ▼
 Message Consumer
       │
       ▼
 Transformation Engine
       │
       ├── Rule-Based Processing
       ├── C# Script Processing
       ├── Spike Detection
       ├── Change Detection
       └── ML.NET Analysis
       │
       ▼
 MQTT Publisher / Alerts / Storage
```

## Warning
* These projects are intended as experiments and learning exercises.
* Error handling, security, and production readiness may vary between PoCs.

## Background
* [Message Transformation in code with HiveMQ Client](https://blog.devmobile.co.nz/2026/02/01/message-transformation-in-code-with-hivemq-client/)

* [Message Transformation with cached transform binaries](https://blog.devmobile.co.nz/2026/03/17/message-transformation-with-cached-transform-binaries/)

* [Real-time IoT Spike and Change Point Detection with ML.NET](https://blog.devmobile.co.nz/2026/05/27/real-time-iot-spike-and-change-point-detection-with-ml-net/)

* [Real-time IoT Change Point Detection with ML.NET](https://blog.devmobile.co.nz/2026/06/16/real-time-iot-change-point-detection-with-ml-net/)

* [Real-time Spike Detection with SR-CNN](https://blog.devmobile.co.nz/2026/08/15/real-time-spike-detection-with-sr-cnn/)

* [Real-time ML.Net Regression with LightGBM]

* [Real-time ML.Net Binary classification with LightGBM]

* [Real-time ML.Met Multi class classification with LightGBM]

The MQTT examples should be considered as "snappy" rather than "real-time" as embedded/edge compute "real-time" is different often with specified values for up-time, duration, latency and jitter.

## License

MIT License

***
