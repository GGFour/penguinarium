## Stage 2 methods

1. Isolation Forest  
   - An unsupervised method that isolates anomalies by randomly splitting features. Points isolated quickly in fewer splits are flagged as anomalies. It is efficient for large datasets.

2. One-Class Support Vector Machine (One-Class SVM)  
   - Learns a boundary around normal data points. Anything outside this boundary is detected as anomalous. Useful when anomalies are rare.

3. k-Nearest Neighbors (k-NN) based anomaly detection  
   - Detects anomalies based on distances to nearest neighbors. Points far from neighbors are considered anomalies, simple but computation-intensive on large data.

4. Local Outlier Factor (LOF)  
   - Measures local density deviation relative to neighbors. Points with lower density compared to neighbors are anomalies.

5. Autoencoders  
   - Neural networks that learn to compress and decompress data. Higher reconstruction error indicates anomalous points, useful for complex, high-dimensional data.

6. Random Forest based anomaly detection  
   - Uses decision trees to learn patterns and differentiate normal vs anomalous data, effective in supervised or semi-supervised settings.

7. K-Means Clustering (Unsupervised)  
   - Groups data by similarity; points far from cluster centers are anomaly candidates.

8. Generative Adversarial Networks (GANs)  
   - Deep learning technique where models learn to generate data and detect anomalies by identifying data points that don't conform to the generated distribution.

9. Long Short-Term Memory Networks (LSTM)  
   - Recurrent neural networks designed to detect anomalies in time series by learning long-term dependencies.

10. Ensemble Methods  
    - Combine predictions from multiple models (e.g., isolation forest, LOF, SVM) to improve robustness and accuracy of anomaly detection.