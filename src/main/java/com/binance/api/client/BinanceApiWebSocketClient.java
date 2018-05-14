package com.binance.api.client;

import com.binance.api.client.domain.event.*;
import com.binance.api.client.domain.market.CandlestickInterval;

import java.util.List;

/**
 * Binance API data streaming façade, supporting streaming of events through web sockets.
 */
public interface BinanceApiWebSocketClient {

  void onDepthEvent(String symbol, BinanceApiCallback<DepthEvent> callback);

  void onCandlestickEvent(String symbol, CandlestickInterval interval, BinanceApiCallback<CandlestickEvent> callback);

  void onAggTradeEvent(String symbol, BinanceApiCallback<AggTradeEvent> callback);

  void onUserDataUpdateEvent(String listenKey, BinanceApiCallback<UserDataUpdateEvent> callback);

  void onAllMarketTickersEvent(BinanceApiCallback<List<AllMarketTickersEvent>> callback);
}
