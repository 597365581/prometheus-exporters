package com.yongqing.prometheus.core;

import io.prometheus.client.exporter.MetricsServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class ExposePrometheusMetricsServer implements AutoCloseable{
    private final Server server;

    public ExposePrometheusMetricsServer(int port, MetricsServlet metricsServlet){
        this.server = new Server(port);
        ServletContextHandler servletContextHandler = new ServletContextHandler();
        servletContextHandler.setContextPath("/");
        server.setHandler(servletContextHandler);
        servletContextHandler.addServlet(new ServletHolder(metricsServlet),"/metrics");
    }

    @Override
    public void close() throws Exception {


    }

    public void start(){
        try{
            server.start();
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
