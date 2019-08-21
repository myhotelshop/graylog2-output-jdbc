package com.wizecore.graylog2.plugin;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.ServerStatus.Capability;
import org.graylog2.plugin.Version;

public class JDBCOutputMetaData implements PluginMetaData {

	@Override
	public String getAuthor() {
		return "Wizecore. Based on work by Intelie.";
	}

	@Override
	public String getDescription() {
		return "Enables sending messages to syslog.";
	}

	@Override
	public String getName() {
		return "JDBCOutput";
	}

	@Override
	public Set<Capability> getRequiredCapabilities() {
		return Collections.emptySet();
	}

	@Override
	public Version getRequiredVersion() {
		return new Version(1, 0, 0);
	}

	@Override
	public URI getURL() {
		return URI.create("https://github.com/wizecore/graylog2-output-jdbc");
	}

	@Override
	public String getUniqueId() {
		return JDBCOutput.class.getName();
	}

	@Override
	public Version getVersion() {
		return new Version(1, 0, 0);
	}
}
