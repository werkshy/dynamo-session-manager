package net.energyhub.session;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Locale;
import java.util.Random;
import java.util.logging.Logger;

/**
 * StatsdClient to post dynamo stats to statsd, entirely optional.
 * Based almost entirely on https://github.com/etsy/statsd/blob/master/examples/StatsdClient.java
 */

public class StatsdClient {
    private String host;
    private Integer port;

    private static final Random RNG = new Random();

    private InetSocketAddress _address;
    private DatagramChannel _channel;

    private String prefix;
    private static Logger log = Logger.getLogger("net.energyhub.session.StatsdClient");


    public StatsdClient(String host, int port) {
        this.host = host;
        this.port = port;
        try {
            this.prefix = "dynamo." + java.net.InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.severe("Could not figure out hostname, using 'unknown'");
            this.prefix = "unknown";
        }
        try {
            _address = new InetSocketAddress(InetAddress.getByName(host), port);
            _channel = DatagramChannel.open();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start StatsD client", e);
        }
        log.info("Creating statsd client for dynamo-session-manager: " + this.host + ":" + this.port);
    }

    /**
     * Convenience function that takes two longs for ms (since that's the easy thing to get in Java.
     * Presumably whatever we're timing falls well within the int range, but check it anyway.
     */
    public void time(String key, long start, long end, double sampleRate) {
        timing(key, delta(start, end), sampleRate);
    }

    public void time(String key, long start, long end) {
        timing(key, delta(start, end), 1.0);
    }

    /**
     * Convenience function that assumes end time is 'now'.
     */
    public void timeSince(String aspect, long start) {
        long end = System.currentTimeMillis();
        time(aspect, start, end, 1.0);
    }

    public void timeSince(String aspect, long start, double sampleRate) {
        long end = System.currentTimeMillis();
        time(aspect, start, end, sampleRate);
    }


    //// Below this line copied from example client --AON

	public boolean timing(String key, int value) {
		return timing(key, value, 1.0);
	}

    public boolean timing(String key, double value) {
        Long lval = Math.round(Math.ceil(value));
        return timing(key, delta(0l, lval));
    }

	public boolean timing(String key, int value, double sampleRate) {
		return send(sampleRate, String.format(Locale.ENGLISH, "%s.%s:%d|ms", prefix, key, value));
	}

	public boolean decrement(String key) {
		return increment(key, -1, 1.0);
	}

	public boolean decrement(String key, int magnitude) {
		return decrement(key, magnitude, 1.0);
	}

	public boolean decrement(String key, int magnitude, double sampleRate) {
		magnitude = magnitude < 0 ? magnitude : -magnitude;
		return increment(key, magnitude, sampleRate);
	}

	public boolean decrement(String... keys) {
		return increment(-1, 1.0, keys);
	}

	public boolean decrement(int magnitude, String... keys) {
		magnitude = magnitude < 0 ? magnitude : -magnitude;
		return increment(magnitude, 1.0, keys);
	}

	public boolean decrement(int magnitude, double sampleRate, String... keys) {
		magnitude = magnitude < 0 ? magnitude : -magnitude;
		return increment(magnitude, sampleRate, keys);
	}

	public boolean increment(String key) {
		return increment(key, 1, 1.0);
	}

	public boolean increment(String key, int magnitude) {
		return increment(key, magnitude, 1.0);
	}

	public boolean increment(String key, int magnitude, double sampleRate) {
		String stat = String.format(Locale.ENGLISH, "%s.%s:%s|c", prefix, key, magnitude);
		return send(sampleRate, stat);
	}

	public boolean increment(int magnitude, double sampleRate, String... keys) {
		String[] stats = new String[keys.length];
		for (int i = 0; i < keys.length; i++) {
			stats[i] = String.format(Locale.ENGLISH, "%s.%s:%s|c", prefix, keys[i], magnitude);
		}
		return send(sampleRate, stats);
	}

	public boolean gauge(String key, double magnitude){
		return gauge(key, magnitude, 1.0);
	}

	public boolean gauge(String key, double magnitude, double sampleRate){
		final String stat = String.format(Locale.ENGLISH, "%s.%s:%s|g", prefix, key, magnitude);
		return send(sampleRate, stat);
	}

	private boolean send(double sampleRate, String... stats) {

		boolean retval = false; // didn't send anything
		if (sampleRate < 1.0) {
			for (String stat : stats) {
				if (RNG.nextDouble() <= sampleRate) {
					stat = String.format(Locale.ENGLISH, "%s|@%f", stat, sampleRate);
					if (doSend(stat)) {
						retval = true;
					}
				}
			}
		} else {
			for (String stat : stats) {
				if (doSend(stat)) {
					retval = true;
				}
			}
		}

		return retval;
	}

	private boolean doSend(final String stat) {
		try {
			final byte[] data = stat.getBytes("utf-8");
			final ByteBuffer buff = ByteBuffer.wrap(data);
			final int nbSentBytes = _channel.send(buff, _address);

			if (data.length == nbSentBytes) {
				return true;
			} else {
				log.severe("Could not send entirely stat " + stat + " to host " + _address.getHostName()
                        + ". Only sent " + nbSentBytes + " bytes out of " + data.length);
				return false;
			}

		} catch (IOException e) {
			log.severe("Could not send stat " + stat + " to host " + _address.getHostName());
            e.printStackTrace();
			return false;
		}
	}

    /**
     * Return an int = t1 -t0
     */
    private int delta(long t0, long t1) {
        long delta = t1 - t0;
        if (delta < Integer.MIN_VALUE || delta > Integer.MAX_VALUE) {
            log.warning("Trying to time something outside of int range: " + t0 + "-" + t1);
            return -1;
        }
        return (int) delta;
    }

}
