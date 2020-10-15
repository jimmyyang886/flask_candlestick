function getData() {
	console.log();
	var chart = echarts.init(document.getElementById('kline'), 'white', {renderer: 'canvas'});
	//var chart = echarts.init(canvas, 'white', {renderer: 'canvas'});
	$.ajax({
		type: "POST",
		dataType: "json",
		url: "/Kline" ,
		data: $('#form1').serialize(),
		success: function (result) {
			chart.setOption(result);
			chart.on('click', function (result) {
				//alert(result.name);
				var click_time = result.name;
				var start_time = time_interval.start.value;
				//click_time.replace(/-/g, "/");
				//start_time.replace(/-/g, "/");
				if (Date.parse(click_time).valueOf() > Date.parse(start_time).valueOf()){
					time_interval.end.value = click_time;
				} else{
					time_interval.start.value = click_time;
				}
			});
		 },
		error: function() {
			alert("	wrong stock code！");
			}
	});
}


function genDataset() {
			console.log();
			var chart = echarts.init(document.getElementById('kline_dataset'), 'white', {renderer: 'canvas'});
			//var chart = echarts.init(canvas, 'white', {renderer: 'canvas'});
			$.ajax({
				type: "POST",
				dataType: "json",
				url: "/Dataset" ,
				data: $('#form2').serialize(),
				success: function (result) {
					chart.setOption(result);
					$('#form2')[0].reset();					
				 },
				error: function() {
					alert("not work！");
					}
			});
		}

		