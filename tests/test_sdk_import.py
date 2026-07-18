from fyers_apiv3.FyersWebsocket.tbt_ws import Depth, SubscriptionModes


def test_fyers_tbt_sdk_import_and_depth_shape() -> None:
    depth = Depth()

    assert len(depth.bidprice) == 50
    assert len(depth.askprice) == 50
    assert SubscriptionModes.DEPTH in list(SubscriptionModes)
