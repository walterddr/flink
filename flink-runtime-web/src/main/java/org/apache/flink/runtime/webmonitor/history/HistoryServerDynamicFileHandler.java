package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoryServerDynamicFileHandler extends SimpleChannelInboundHandler<RoutedRequest> {

	private static final Logger LOG = LoggerFactory.getLogger(HistoryServerDynamicFileHandler.class);

	private final Configuration config;

	public HistoryServerDynamicFileHandler(Configuration config) {
		this.config = config;
	}


	@Override
	protected void channelRead0(ChannelHandlerContext ctx, RoutedRequest routedRequest) throws Exception {
		String requestPath = routedRequest.getPath();
		responseWithExternalFileService(ctx, routedRequest.getRequest(), requestPath);
	}

	private void responseWithExternalFileService(ChannelHandlerContext ctx, HttpRequest request, String requestPath) {

	}
}
